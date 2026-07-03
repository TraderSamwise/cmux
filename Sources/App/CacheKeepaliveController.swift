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
    private var lastInjectionByPanel: [AgentHibernationPanelKey: TimeInterval] = [:]

    private init() {}

    /// Called when Claude completes a turn (Stop hook fired). This is the unambiguous
    /// "agent is done" signal — no lifecycle inference needed.
    func handleTurnCompleted(workspaceId: UUID, panelId: UUID) {
        guard CacheKeepaliveSettings.isEnabled() else { return }
        let key = AgentHibernationPanelKey(workspaceId: workspaceId, panelId: panelId)

        // Reset ping counter if this is a real user turn (>5s since our last injection)
        if let lastInjection = lastInjectionByPanel[key],
           Date().timeIntervalSince1970 - lastInjection > 5.0 {
            pingCountByPanel.removeValue(forKey: key)
        }

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

        // Check transcript eligibility
        guard transcriptExceedsThreshold(workspaceId: key.workspaceId, panelId: key.panelId) else {
            pingCountByPanel.removeValue(forKey: key)
            return
        }

        let count = pingCountByPanel[key] ?? 0
        let maxPings = CacheKeepaliveSettings.maxPings()

        lastInjectionByPanel[key] = Date().timeIntervalSince1970

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

    // MARK: - Transcript threshold

    private func transcriptExceedsThreshold(workspaceId: UUID, panelId: UUID) -> Bool {
        let bytesNeeded = CacheKeepaliveSettings.minTranscriptBytes()
        guard let path = transcriptPath(workspaceId: workspaceId, panelId: panelId) else {
            return false
        }
        return measureBytesSinceLastCompact(path: path) >= bytesNeeded
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

        let searchSize = min(fileSize, 4 * 1024 * 1024)
        let searchOffset = fileSize - searchSize
        handle.seek(toFileOffset: UInt64(searchOffset))
        let chunk = handle.readData(ofLength: searchSize)

        guard let needle = "compact_boundary".data(using: .utf8) else { return fileSize }

        if let range = chunk.range(of: needle, options: .backwards) {
            return fileSize - (searchOffset + range.lowerBound)
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
