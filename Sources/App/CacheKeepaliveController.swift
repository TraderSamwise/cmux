import Foundation
import os

private let logger = Logger(subsystem: "com.cmuxterm.app", category: "CacheKeepalive")

// MARK: - Settings (read from ~/.config/ghostty/config)

enum CacheKeepaliveSettings {
    static func isEnabled() -> Bool {
        GhosttyConfig.load().cacheKeepaliveEnabled
    }

    static func idleSeconds() -> TimeInterval {
        GhosttyConfig.load().cacheKeepaliveIdleSeconds
    }

    static func minTranscriptBytes() -> Int {
        GhosttyConfig.load().cacheKeepaliveMinTranscriptBytes
    }

    static func maxPings() -> Int {
        GhosttyConfig.load().cacheKeepaliveMaxPings
    }

    static func pingMessage() -> String {
        GhosttyConfig.load().cacheKeepalivePingMessage
    }
}

// MARK: - Controller

/// Keeps Claude Code's prompt cache warm by injecting keepalive pings into idle sessions,
/// then compacts when pings are exhausted.
///
/// Triggered by a dedicated `cache_keepalive_turn_complete` socket command sent from the
/// CLI's Stop hook handler — not by lifecycle values (which conflate "started working" with
/// "finished with background shell"). Following aimux's model: Stop fired = turn done = idle.
@MainActor
final class CacheKeepaliveController {
    static let shared = CacheKeepaliveController()

    private struct PendingAction {
        let timer: DispatchWorkItem
    }

    private var pendingByPanel: [AgentHibernationPanelKey: PendingAction] = [:]
    private var pingCountByPanel: [AgentHibernationPanelKey: Int] = [:]

    private init() {}

    func debugLog(_ message: String) {
        let path = (NSHomeDirectory() as NSString).appendingPathComponent(".cache/cmux-keepalive-debug.log")
        let line = "\(ISO8601DateFormatter().string(from: Date())) \(message)\n"
        if let handle = FileHandle(forWritingAtPath: path) {
            handle.seekToEndOfFile()
            handle.write(line.data(using: .utf8)!)
            handle.closeFile()
        } else {
            FileManager.default.createFile(atPath: path, contents: line.data(using: .utf8))
        }
    }

    /// Called when Claude completes a turn (Stop hook fired).
    func handleTurnCompleted(workspaceId: UUID, panelId: UUID) {
        debugLog("handleTurnCompleted ws=\(workspaceId.uuidString.prefix(8)) panel=\(panelId.uuidString.prefix(8))")
        guard CacheKeepaliveSettings.isEnabled() else {
            debugLog("SKIP: not enabled")
            return
        }
        let key = AgentHibernationPanelKey(workspaceId: workspaceId, panelId: panelId)

        // Check if last response was just "." — our ping's ack
        if lastAssistantResponseIsPingAck(workspaceId: workspaceId, panelId: panelId) {
            debugLog("ping ack detected, continuing cycle")
            scheduleAction(key: key)
            return
        }

        // Real user turn — reset counter, start fresh keepalive cycle
        debugLog("real user turn, resetting counter")
        pingCountByPanel.removeValue(forKey: key)
        scheduleAction(key: key)
    }

    private func scheduleAction(key: AgentHibernationPanelKey) {
        cancelAction(key: key)

        let delay = CacheKeepaliveSettings.idleSeconds()
        let workItem = DispatchWorkItem { [weak self] in
            Task { @MainActor in
                self?.fireAction(key: key)
            }
        }

        pendingByPanel[key] = PendingAction(timer: workItem)
        DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: workItem)
    }

    private func cancelAction(key: AgentHibernationPanelKey) {
        if let pending = pendingByPanel.removeValue(forKey: key) {
            pending.timer.cancel()
        }
    }

    private func fireAction(key: AgentHibernationPanelKey) {
        pendingByPanel.removeValue(forKey: key)

        guard let appDelegate = AppDelegate.shared else { return }

        guard let (_, terminalPanel) = appDelegate.findTerminalPanel(
            workspaceId: key.workspaceId,
            panelId: key.panelId
        ) else { return }

        // Don't inject if agent is mid-turn (transcript-based, same as aimux)
        guard isAgentIdle(workspaceId: key.workspaceId, panelId: key.panelId) else {
            debugLog("fireAction: agent mid-turn, rescheduling")
            scheduleAction(key: key)
            return
        }

        // Check transcript eligibility
        let transcriptBytes = measureTranscriptBytes(workspaceId: key.workspaceId, panelId: key.panelId)
        let bytesNeeded = CacheKeepaliveSettings.minTranscriptBytes()
        guard transcriptBytes >= bytesNeeded else {
            debugLog("fireAction: transcript \(transcriptBytes) < threshold \(bytesNeeded), skipping")
            pingCountByPanel.removeValue(forKey: key)
            return
        }

        let count = pingCountByPanel[key] ?? 0
        let maxPings = CacheKeepaliveSettings.maxPings()
        debugLog("fireAction: count=\(count) maxPings=\(maxPings)")

        if count >= maxPings {
            logger.info("injecting /compact (after \(count) pings)")
            terminalPanel.sendInput("/compact\n")
            pingCountByPanel.removeValue(forKey: key)
        } else {
            let message = CacheKeepaliveSettings.pingMessage()
            logger.info("injecting ping \(count + 1)/\(maxPings)")
            terminalPanel.sendInput(message + "\n")
            pingCountByPanel[key] = count + 1
        }
    }

    // MARK: - Turn state (transcript-based, aimux model)

    /// Reads the transcript tail to determine if the agent is idle.
    /// Idle = last assistant record has stop_reason "end_turn" with no user record after it.
    private func isAgentIdle(workspaceId: UUID, panelId: UUID) -> Bool {
        guard let path = transcriptPath(workspaceId: workspaceId, panelId: panelId) else {
            return true // No transcript = assume idle (let other checks gate)
        }
        guard let handle = FileHandle(forReadingAtPath: path) else { return true }
        defer { handle.closeFile() }

        guard let attrs = try? FileManager.default.attributesOfItem(atPath: path),
              let fileSize = (attrs[.size] as? NSNumber)?.intValue,
              fileSize > 0 else { return true }

        // Read last 256KB (aimux uses this size to account for trailing bookkeeping)
        let tailSize = min(fileSize, 256 * 1024)
        handle.seek(toFileOffset: UInt64(fileSize - tailSize))
        let chunk = handle.readData(ofLength: tailSize)
        guard let text = String(data: chunk, encoding: .utf8) else { return true }

        let lines = text.components(separatedBy: "\n").reversed()
        var sawUserAfterAssistant = false

        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty,
                  let data = trimmed.data(using: .utf8),
                  let record = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let type = record["type"] as? String else { continue }

            if type == "user" {
                sawUserAfterAssistant = true
                continue
            }
            guard type == "assistant" else { continue }

            // Found last assistant record
            let message = record["message"] as? [String: Any]
            let stopReason = message?["stop_reason"] as? String

            if stopReason == "tool_use" || stopReason == "pause_turn" {
                return false // mid-turn
            }
            // Terminal stop reason — idle unless a user record started a new turn
            return !sawUserAfterAssistant
        }

        return true // No assistant record found, assume idle
    }

    // MARK: - Ping ack detection

    /// Checks if the last assistant response in the transcript is just "." (our ping ack).
    private func lastAssistantResponseIsPingAck(workspaceId: UUID, panelId: UUID) -> Bool {
        guard let path = transcriptPath(workspaceId: workspaceId, panelId: panelId) else {
            return false
        }
        guard let handle = FileHandle(forReadingAtPath: path) else { return false }
        defer { handle.closeFile() }

        guard let attrs = try? FileManager.default.attributesOfItem(atPath: path),
              let fileSize = (attrs[.size] as? NSNumber)?.intValue,
              fileSize > 0 else { return false }

        // Read last 8KB — enough to find the last assistant message
        let searchSize = min(fileSize, 8 * 1024)
        handle.seek(toFileOffset: UInt64(fileSize - searchSize))
        let chunk = handle.readData(ofLength: searchSize)

        guard let text = String(data: chunk, encoding: .utf8) else { return false }

        // JSONL: find last line with "role":"assistant"
        let lines = text.components(separatedBy: "\n").reversed()
        for line in lines {
            guard line.contains("\"role\":\"assistant\"") || line.contains("\"role\": \"assistant\"") else {
                continue
            }
            // Check if the content is just "." (possibly with whitespace)
            // Claude's response in JSONL has content array with text blocks
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.contains("\"text\":\".\"") || trimmed.contains("\"text\": \".\"") {
                return true
            }
            // Also check for content that's just a period with possible whitespace
            if let range = line.range(of: "\"text\":\"") ?? line.range(of: "\"text\": \"") {
                let afterText = line[range.upperBound...]
                if let endQuote = afterText.firstIndex(of: "\"") {
                    let content = String(afterText[afterText.startIndex..<endQuote])
                        .trimmingCharacters(in: .whitespaces)
                    if content == "." {
                        return true
                    }
                }
            }
            // Found the last assistant message but it's not just "."
            return false
        }

        return false
    }

    // MARK: - Transcript threshold

    private func measureTranscriptBytes(workspaceId: UUID, panelId: UUID) -> Int {
        guard let path = transcriptPath(workspaceId: workspaceId, panelId: panelId) else {
            return 0
        }
        return measureBytesSinceLastCompact(path: path)
    }

    private func transcriptPath(workspaceId: UUID, panelId: UUID) -> String? {
        let hookSessionsPath = (NSHomeDirectory() as NSString)
            .appendingPathComponent(".cmuxterm/claude-hook-sessions.json")

        guard let data = FileManager.default.contents(atPath: hookSessionsPath),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let activeByWorkspace = json["activeSessionsByWorkspace"] as? [String: Any],
              let sessions = json["sessions"] as? [String: Any] else {
            return nil
        }

        let wsKey = activeByWorkspace.keys.first {
            $0.caseInsensitiveCompare(workspaceId.uuidString) == .orderedSame
        }
        guard let wsKey,
              let wsEntry = activeByWorkspace[wsKey] as? [String: Any],
              let sessionId = wsEntry["sessionId"] as? String,
              let session = sessions[sessionId] as? [String: Any],
              let path = session["transcriptPath"] as? String,
              FileManager.default.fileExists(atPath: path) else {
            return nil
        }

        return path
    }

    private func measureBytesSinceLastCompact(path: String) -> Int {
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: path),
              let fileSize = (attrs[.size] as? NSNumber)?.intValue,
              fileSize > 0 else { return 0 }

        guard let handle = FileHandle(forReadingAtPath: path) else { return fileSize }
        defer { handle.closeFile() }

        // Match statusline.sh: grep for compact_boundary, measure from last occurrence
        let searchSize = min(fileSize, 4 * 1024 * 1024)
        let searchOffset = fileSize - searchSize
        handle.seek(toFileOffset: UInt64(searchOffset))
        let chunk = handle.readData(ofLength: searchSize)

        guard let needle = "compact_boundary".data(using: .utf8) else { return fileSize }

        if let range = chunk.range(of: needle, options: .backwards) {
            // Find start of the line containing the marker (same as tail -n +LINE)
            let lineStart = chunk[..<range.lowerBound].lastIndex(of: UInt8(ascii: "\n"))
                .map { chunk.index(after: $0) } ?? chunk.startIndex
            return fileSize - (searchOffset + chunk.distance(from: chunk.startIndex, to: lineStart))
        }

        return fileSize
    }
}

extension AppDelegate {
    @MainActor
    func findTerminalPanel(workspaceId: UUID, panelId: UUID) -> (Workspace, TerminalPanel)? {
        for context in mainWindowContexts.values {
            for workspace in context.tabManager.tabs where workspace.id == workspaceId {
                if let panel = workspace.panels[panelId] as? TerminalPanel {
                    return (workspace, panel)
                }
            }
        }
        if let tabManager {
            for workspace in tabManager.tabs where workspace.id == workspaceId {
                if let panel = workspace.panels[panelId] as? TerminalPanel {
                    return (workspace, panel)
                }
            }
        }
        return nil
    }
}
