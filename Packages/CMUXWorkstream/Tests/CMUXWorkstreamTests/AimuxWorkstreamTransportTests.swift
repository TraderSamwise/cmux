import Foundation
import Testing
@testable import CMUXWorkstream

@MainActor
@Suite("AimuxWorkstreamTransport")
struct AimuxWorkstreamTransportTests {
    @Test("maps Feed permission decisions to aimux decision strings")
    func decisionMapping() {
        #expect(AimuxWorkstreamTransport.aimuxDecision(for: .permission(.once)) == "allow_once")
        #expect(AimuxWorkstreamTransport.aimuxDecision(for: .permission(.always)) == "allow_always")
        #expect(AimuxWorkstreamTransport.aimuxDecision(for: .permission(.all)) == "allow_always")
        #expect(AimuxWorkstreamTransport.aimuxDecision(for: .permission(.bypass)) == "allow_always")
        #expect(AimuxWorkstreamTransport.aimuxDecision(for: .permission(.deny)) == "deny")
        // aimux models only the permission type today.
        #expect(AimuxWorkstreamTransport.aimuxDecision(for: .question(selections: ["x"])) == nil)
    }

    @Test("builds an actionable aimux permission event")
    func permissionEvent() {
        let e = AimuxWorkstreamTransport.permissionEvent(
            sessionId: "s1", requestId: "req-1", toolName: "Bash", summary: "Bash: rm -rf build")
        #expect(e.hookEventName == .permissionRequest)
        #expect(e.source == "aimux")
        #expect(e.requestId == "req-1")
        #expect(e.sessionId == "s1")
        #expect(e.toolName == "Bash")
        #expect(e.toolInputJSON?.contains("rm -rf build") == true)
    }

    @Test("derives tool name from a 'Tool: detail' summary when not explicit")
    func toolNameFromSummary() {
        let e = AimuxWorkstreamTransport.permissionEvent(
            sessionId: "s1", requestId: "r", toolName: nil, summary: "Edit: /a/b.ts")
        #expect(e.toolName == "Edit")
    }

    @Test("falls back to 'permission' with no tool name or summary")
    func toolNameFallback() {
        let e = AimuxWorkstreamTransport.permissionEvent(
            sessionId: "s1", requestId: "r", toolName: nil, summary: nil)
        #expect(e.toolName == "permission")
    }

    @Test("an aimux event ingests into the store as a pending actionable card")
    func ingestsAsPendingCard() {
        let store = WorkstreamStore(ringCapacity: 10)
        store.ingest(AimuxWorkstreamTransport.permissionEvent(
            sessionId: "glyde", requestId: "req-9", toolName: "Bash", summary: "Bash: ls"))
        #expect(store.items.count == 1)
        #expect(store.pending.count == 1)
        let item = store.items[0]
        #expect(item.source == .aimux)
        #expect(item.kind == .permissionRequest)
        if case .permissionRequest(let rid, let tool, _, _) = item.payload {
            #expect(rid == "req-9")
            #expect(tool == "Bash")
        } else {
            Issue.record("expected permissionRequest payload")
        }
    }
}
