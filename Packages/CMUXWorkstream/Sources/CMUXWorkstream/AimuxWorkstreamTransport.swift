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
                // Dispatch per `data:` line: AsyncBytes.lines does not reliably
                // yield the blank SSE delimiter, so we can't wait for an empty
                // line. Each aimux frame is one `event:` + one `data:` line.
                var eventName = ""
                for try await line in bytes.lines {
                    if Task.isCancelled { break }
                    if line.hasPrefix("event:") {
                        eventName = String(line.dropFirst("event:".count)).trimmingCharacters(in: .whitespaces)
                    } else if line.hasPrefix("data:") {
                        let payload = String(line.dropFirst("data:".count)).trimmingCharacters(in: .whitespaces)
                        handleFrame(event: eventName, data: payload, endpoint: endpoint)
                        eventName = ""
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
        switch event {
        case "ready":
            // The ready snapshot carries each pending entry's full tool input.
            guard let d = data.data(using: .utf8),
                  let obj = try? JSONSerialization.jsonObject(with: d) as? [String: Any],
                  let pending = obj["pending"] as? [[String: Any]] else { return }
            for entry in pending { emitEntry(entry, endpoint: endpoint) }
        case "interaction":
            let obj = (data.data(using: .utf8)).flatMap {
                try? JSONSerialization.jsonObject(with: $0) as? [String: Any]
            }
            let interaction = obj?["interaction"] as? [String: Any]
            if (interaction?["telemetry"] as? Bool) == true {
                // Read-only notice (Codex): render a non-actionable row directly
                // from the alert — there is no pending entry to fetch.
                emitTelemetry(
                    sessionId: (obj?["sessionId"] as? String) ?? (interaction?["id"] as? String) ?? "aimux",
                    requestId: (interaction?["id"] as? String) ?? UUID().uuidString,
                    toolName: (interaction?["toolName"] as? String) ?? "permission",
                    toolInputJSON: (interaction?["toolInputJSON"] as? String) ?? "{}",
                    cwd: obj?["worktreePath"] as? String
                )
            } else {
                // Actionable: the push carries only a summary; fetch the
                // authoritative pending list for the full tool input.
                Task { [weak self] in await self?.fetchAndEmitPending(endpoint) }
            }
        default:
            break
        }
    }

    private func fetchAndEmitPending(_ endpoint: Endpoint) async {
        guard let url = URL(string: "\(endpoint.base)/agents/interaction/pending"),
              let (d, resp) = try? await session.data(from: url),
              (resp as? HTTPURLResponse)?.statusCode == 200,
              let obj = try? JSONSerialization.jsonObject(with: d) as? [String: Any],
              let requests = obj["requests"] as? [[String: Any]] else { return }
        for entry in requests { emitEntry(entry, endpoint: endpoint) }
    }

    /// Emits a permission event from a registry entry, preserving the agent's
    /// real `tool_input` (so cmux renders Bash commands, Edit diffs, etc. as it
    /// does for native agents) rather than a summary blob.
    private func emitEntry(_ entry: [String: Any], endpoint: Endpoint) {
        guard (entry["type"] as? String) == "permission", let id = entry["id"] as? String else { return }
        let sessionId = (entry["sessionId"] as? String) ?? id
        let payload = entry["payload"] as? [String: Any]
        // The agent's working dir (worktree, or project root if none) rides in
        // the payload; fall back to projectRoot. Drives the project/worktree label.
        let cwd = (payload?["cwd"] as? String) ?? (entry["projectRoot"] as? String)
        let toolName = (payload?["toolName"] as? String) ?? "permission"
        let input = payload?["input"] ?? [String: Any]()
        let inputJSON = (try? JSONSerialization.data(withJSONObject: input))
            .flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
        emit(sessionId: sessionId, requestId: id, toolName: toolName, toolInputJSON: inputJSON, cwd: cwd, endpoint: endpoint)
    }

    private func emit(sessionId: String, requestId: String, toolName: String, toolInputJSON: String, cwd: String?, endpoint: Endpoint) {
        endpointForRequest[requestId] = endpoint
        guard !emittedRequestIds.contains(requestId) else { return }
        emittedRequestIds.insert(requestId)
        onEvent?(Self.permissionEvent(sessionId: sessionId, requestId: requestId, toolName: toolName, toolInputJSON: toolInputJSON, cwd: cwd))
    }

    /// Emits a non-actionable read-only row (e.g. Codex permission, whose native
    /// TUI owns the decision). `.preToolUse` maps to a telemetry `toolUse` item.
    private func emitTelemetry(sessionId: String, requestId: String, toolName: String, toolInputJSON: String, cwd: String?) {
        guard !emittedRequestIds.contains(requestId) else { return }
        emittedRequestIds.insert(requestId)
        onEvent?(WorkstreamEvent(
            sessionId: sessionId,
            hookEventName: .preToolUse,
            source: WorkstreamSource.aimux.rawValue,
            cwd: cwd,
            toolName: toolName,
            toolInputJSON: toolInputJSON,
            requestId: requestId
        ))
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

    /// Builds an actionable permission `WorkstreamEvent`, carrying the agent's
    /// real `tool_input` JSON so the Feed renders it like a native agent's card.
    static func permissionEvent(sessionId: String, requestId: String, toolName: String, toolInputJSON: String, cwd: String? = nil) -> WorkstreamEvent {
        WorkstreamEvent(
            sessionId: sessionId,
            hookEventName: .permissionRequest,
            source: WorkstreamSource.aimux.rawValue,
            cwd: cwd,
            toolName: toolName,
            toolInputJSON: toolInputJSON,
            requestId: requestId
        )
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
}
