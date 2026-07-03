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
/// then compacts before the cache TTL expires. Driven by agent lifecycle events from cmux's
/// Claude wrapper hooks.
///
/// Flow:
/// 1. Agent goes idle → timer starts (idleSeconds, default 240s)
/// 2. Timer fires → check eligibility (transcript > minTranscriptBytes) → inject ping message
/// 3. Claude responds → idle → repeat up to maxPings times
/// 4. After maxPings → inject /compact
/// 5. After compact → transcript drops below threshold → naturally stops
/// 6. User works conversation back above threshold → cycle restarts
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

    /// Called when an agent lifecycle changes. Starts or cancels the keepalive timer.
    func handleLifecycleChange(
        workspaceId: UUID,
        panelId: UUID,
        lifecycle: AgentHibernationLifecycleState
    ) {
        guard CacheKeepaliveSettings.isEnabled() else { return }
        let key = AgentHibernationPanelKey(workspaceId: workspaceId, panelId: panelId)

        if lifecycle == .idle {
            scheduleAction(key: key)
        } else if lifecycle == .running {
            cancelAction(key: key)
            // If running transition wasn't caused by our injection, reset ping counter
            if let lastInjection = lastInjectionByPanel[key],
               Date().timeIntervalSince1970 - lastInjection > 5.0 {
                pingCountByPanel.removeValue(forKey: key)
            }
        } else {
            cancelAction(key: key)
        }
    }

    /// Called when the user types in a terminal. Not used for cancellation (we always inject
    /// regardless of draft state), but tracked for diagnostics.
    func handleTerminalInput(workspaceId: UUID, panelId: UUID) {
        // No-op: we inject regardless of user drafting state.
        // The timer is anchored to last API call (idle event), not keystrokes.
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

        guard let (workspace, terminalPanel) = appDelegate.findTerminalPanel(
            workspaceId: key.workspaceId,
            panelId: key.panelId
        ) else { return }

        // Verify still idle
        let lifecycle = workspace.agentHibernationLifecycleState(
            panelId: key.panelId,
            fallback: nil
        )
        guard lifecycle == .idle else { return }

        // Check transcript eligibility
        let eligible = transcriptExceedsThreshold(workspaceId: key.workspaceId, panelId: key.panelId)
        guard eligible else {
            // Below threshold — reset counter (conversation may have been compacted)
            pingCountByPanel.removeValue(forKey: key)
            return
        }

        let count = pingCountByPanel[key] ?? 0
        let maxPings = CacheKeepaliveSettings.maxPings()

        lastInjectionByPanel[key] = Date().timeIntervalSince1970

        if count >= maxPings {
            // Exhausted pings — compact
            logger.info("injecting /compact (after \(count) pings)")
            terminalPanel.sendInput("/compact\n")
            pingCountByPanel.removeValue(forKey: key)
        } else {
            // Send keepalive ping
            let message = CacheKeepaliveSettings.pingMessage()
            logger.info("injecting ping \(count + 1)/\(maxPings)")
            terminalPanel.sendInput(message + "\n")
            pingCountByPanel[key] = count + 1
        }
    }

    private func transcriptExceedsThreshold(workspaceId: UUID, panelId: UUID) -> Bool {
        guard let snapshot = SharedLiveAgentIndex.shared.snapshot(workspaceId: workspaceId, panelId: panelId),
              !snapshot.sessionId.isEmpty else {
            return false
        }

        let stateDir = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude")
            .appendingPathComponent("cache-keepalive")
            .appendingPathComponent(snapshot.sessionId)

        let eligibleFile = stateDir.appendingPathComponent("eligible")

        guard let content = try? String(contentsOf: eligibleFile, encoding: .utf8)
            .trimmingCharacters(in: .whitespacesAndNewlines) else {
            return false
        }

        return content == "1"
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
