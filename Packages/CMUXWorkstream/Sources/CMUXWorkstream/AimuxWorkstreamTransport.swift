import Foundation

/// Streams aimux-managed agents' permission interactions into the Feed and
/// routes decisions back to the aimux daemon over HTTP.
///
/// Inbound: polls the aimux daemon `/projects`, opens an SSE
/// `/agents/interaction/stream` per live project service, and emits a
/// `WorkstreamEvent` (`source = aimux`, `hookEventName = .permissionRequest`)
/// for each pending interaction. Outbound: `respond(requestId:decision:)`
/// POSTs to that project's `/agents/interaction/respond`. cmux's own
/// hook-sourced items are unaffected — they never flow through a transport.
public actor AimuxWorkstreamTransport: WorkstreamTransport {
    struct Endpoint: Sendable, Equatable {
        let host: String
        let port: Int
        var base: String { "http://\(host):\(port)" }
        var key: String { "\(host):\(port)" }
    }

    private let daemonBaseURL: URL
    private let session: URLSession
    private let pollIntervalSeconds: UInt64
    private var onEvent: (@Sendable (WorkstreamEvent) -> Void)?
    private var pollTask: Task<Void, Never>?
    private var streamTasks: [String: Task<Void, Never>] = [:]
    private var endpointForRequest: [String: Endpoint] = [:]
    private var emittedRequestIds: Set<String> = []

    public init(daemonBaseURL: URL? = nil, session: URLSession = .shared, pollIntervalSeconds: UInt64 = 5) {
        let envURL = ProcessInfo.processInfo.environment["AIMUX_DAEMON_URL"]
        self.daemonBaseURL = daemonBaseURL
            ?? envURL.flatMap(URL.init(string:))
            ?? URL(string: "http://127.0.0.1:43190")!
        self.session = session
        self.pollIntervalSeconds = pollIntervalSeconds
    }

    // MARK: WorkstreamTransport

    public func subscribe(onEvent: @escaping @Sendable (WorkstreamEvent) -> Void) async throws {
        self.onEvent = onEvent
        guard pollTask == nil else { return }
        pollTask = Task { [weak self] in await self?.runPollLoop() }
    }

    /// aimux decisions are routed via `respond(requestId:decision:)` (which has
    /// the aimux request id); the generic action path is a no-op here.
    public func send(_ action: WorkstreamAction) async throws {}

    public func stop() {
        pollTask?.cancel()
        pollTask = nil
        for task in streamTasks.values { task.cancel() }
        streamTasks.removeAll()
    }

    /// POST the user's decision to the aimux project that owns `requestId`.
    public func respond(requestId: String, decision: WorkstreamDecision) async {
        guard let endpoint = endpointForRequest[requestId],
              let value = Self.aimuxDecision(for: decision),
              let url = URL(string: "\(endpoint.base)/agents/interaction/respond")
        else { return }
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "content-type")
        req.httpBody = try? JSONSerialization.data(
            withJSONObject: ["id": requestId, "response": ["decision": value]]
        )
        _ = try? await session.data(for: req)
        endpointForRequest[requestId] = nil
    }

    // MARK: Poll loop

    private func runPollLoop() async {
        while !Task.isCancelled {
            await pollOnce()
            try? await Task.sleep(nanoseconds: pollIntervalSeconds * 1_000_000_000)
        }
    }

    private func pollOnce() async {
        guard let projects = await fetchProjects() else { return }
        let live = projects.compactMap { $0.liveEndpoint }
        let liveKeys = Set(live.map(\.key))
        for endpoint in live where streamTasks[endpoint.key] == nil {
            streamTasks[endpoint.key] = Task { [weak self] in await self?.runStream(endpoint) }
        }
        for (key, task) in streamTasks where !liveKeys.contains(key) {
            task.cancel()
            streamTasks[key] = nil
        }
    }

    private func fetchProjects() async -> [ProjectInfo]? {
        guard let url = URL(string: "\(daemonBaseURL.absoluteString)/projects"),
              let (data, response) = try? await session.data(from: url),
              (response as? HTTPURLResponse)?.statusCode == 200,
              let decoded = try? JSONDecoder().decode(ProjectsResponse.self, from: data)
        else { return nil }
        return decoded.projects
    }

    // MARK: Per-project SSE

    private func runStream(_ endpoint: Endpoint) async {
        guard let url = URL(string: "\(endpoint.base)/agents/interaction/stream") else { return }
        while !Task.isCancelled {
            do {
                var req = URLRequest(url: url)
                req.timeoutInterval = 3600
                let (bytes, response) = try await session.bytes(for: req)
                guard (response as? HTTPURLResponse)?.statusCode == 200 else {
                    try? await Task.sleep(nanoseconds: 2_000_000_000)
                    continue
                }
                var eventName = ""
                var dataBuffer = ""
                for try await line in bytes.lines {
                    if Task.isCancelled { break }
                    if line.isEmpty {
                        handleFrame(event: eventName, data: dataBuffer, endpoint: endpoint)
                        eventName = ""
                        dataBuffer = ""
                    } else if line.hasPrefix("event:") {
                        eventName = String(line.dropFirst("event:".count)).trimmingCharacters(in: .whitespaces)
                    } else if line.hasPrefix("data:") {
                        dataBuffer += String(line.dropFirst("data:".count)).trimmingCharacters(in: .whitespaces)
                    }
                }
            } catch {
                // connection dropped or cancelled — fall through to retry
            }
            if Task.isCancelled { break }
            try? await Task.sleep(nanoseconds: 2_000_000_000)
        }
    }

    private func handleFrame(event: String, data: String, endpoint: Endpoint) {
        guard let bytes = data.data(using: .utf8) else { return }
        switch event {
        case "ready":
            guard let ready = try? JSONDecoder().decode(StreamReady.self, from: bytes) else { return }
            for pending in ready.pending where pending.type == "permission" {
                emit(sessionId: pending.sessionId, requestId: pending.id,
                     toolName: pending.payload?.toolName, summary: pending.payload?.summary,
                     endpoint: endpoint)
            }
        case "interaction":
            guard let alert = try? JSONDecoder().decode(InteractionAlert.self, from: bytes),
                  alert.interaction.type == "permission" else { return }
            emit(sessionId: alert.sessionId ?? alert.interaction.id, requestId: alert.interaction.id,
                 toolName: nil, summary: alert.interaction.summary, endpoint: endpoint)
        default:
            break
        }
    }

    private func emit(sessionId: String, requestId: String, toolName: String?, summary: String?, endpoint: Endpoint) {
        endpointForRequest[requestId] = endpoint
        guard !emittedRequestIds.contains(requestId) else { return }
        emittedRequestIds.insert(requestId)
        onEvent?(Self.permissionEvent(sessionId: sessionId, requestId: requestId, toolName: toolName, summary: summary))
    }

    // MARK: Pure mappers (unit-tested)

    /// Maps a Feed decision to aimux's response decision string, or nil for
    /// interaction types aimux doesn't model yet (exit-plan/question).
    static func aimuxDecision(for decision: WorkstreamDecision) -> String? {
        switch decision {
        case .permission(let mode):
            switch mode {
            case .once: return "allow_once"
            case .always, .all, .bypass: return "allow_always"
            case .deny: return "deny"
            }
        case .exitPlan, .question:
            return nil
        }
    }

    /// Builds an actionable permission `WorkstreamEvent` from aimux fields.
    /// `toolName` falls back to the prefix of "Tool: detail" summaries.
    static func permissionEvent(sessionId: String, requestId: String, toolName: String?, summary: String?) -> WorkstreamEvent {
        let resolvedTool = toolName
            ?? summary.flatMap { $0.split(separator: ":", maxSplits: 1).first.map { String($0).trimmingCharacters(in: .whitespaces) } }
            ?? "permission"
        let detail = summary ?? resolvedTool
        return WorkstreamEvent(
            sessionId: sessionId,
            hookEventName: .permissionRequest,
            source: WorkstreamSource.aimux.rawValue,
            toolName: resolvedTool,
            toolInputJSON: "{\"summary\":\(jsonStringLiteral(detail))}",
            requestId: requestId
        )
    }

    private static func jsonStringLiteral(_ s: String) -> String {
        guard let data = try? JSONSerialization.data(withJSONObject: s, options: .fragmentsAllowed),
              let str = String(data: data, encoding: .utf8) else { return "\"\"" }
        return str
    }

    // MARK: Wire DTOs

    private struct ProjectsResponse: Decodable { let projects: [ProjectInfo] }

    struct ProjectInfo: Decodable {
        let serviceAlive: Bool?
        let serviceEndpoint: EndpointDTO?
        var liveEndpoint: Endpoint? {
            guard serviceAlive == true, let e = serviceEndpoint else { return nil }
            return Endpoint(host: e.host, port: e.port)
        }
    }

    struct EndpointDTO: Decodable { let host: String; let port: Int }

    private struct StreamReady: Decodable { let pending: [PendingInteraction] }

    private struct PendingInteraction: Decodable {
        let id: String
        let sessionId: String
        let type: String?
        let payload: PendingPayload?
    }

    private struct PendingPayload: Decodable {
        let toolName: String?
        let summary: String?
    }

    private struct InteractionAlert: Decodable {
        let sessionId: String?
        let interaction: AlertInteraction
    }

    private struct AlertInteraction: Decodable {
        let id: String
        let type: String?
        let summary: String?
    }
}
