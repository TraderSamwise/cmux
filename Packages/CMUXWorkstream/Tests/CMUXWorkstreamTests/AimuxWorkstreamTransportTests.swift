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
        #expect(AimuxWorkstreamTransport.aimuxDecision(for: .question(selections: ["x"])) == nil)
    }

    @Test("maps Feed decisions to aimux response objects")
    func responseMapping() {
        let permission = AimuxWorkstreamTransport.aimuxResponse(for: .permission(.once))
        #expect(permission?["decision"] as? String == "allow_once")

        let question = AimuxWorkstreamTransport.aimuxResponse(for: .question(selections: ["branch-a"]))
        #expect(question?["selection"] as? [String] == ["branch-a"])
    }

    @Test("builds an actionable aimux permission event carrying the real tool input")
    func permissionEvent() {
        let e = AimuxWorkstreamTransport.permissionEvent(
            sessionId: "s1", requestId: "req-1", toolName: "Bash",
            toolInputJSON: "{\"command\":\"rm -rf build\"}")
        #expect(e.hookEventName == .permissionRequest)
        #expect(e.source == "aimux")
        #expect(e.requestId == "req-1")
        #expect(e.sessionId == "s1")
        #expect(e.toolName == "Bash")
        // The real tool_input is preserved so the Feed renders the command.
        #expect(e.toolInputJSON == "{\"command\":\"rm -rf build\"}")
    }

    @Test("builds an actionable aimux question event carrying the real question input")
    func questionEvent() {
        let e = AimuxWorkstreamTransport.questionEvent(
            sessionId: "s1", requestId: "req-q",
            toolInputJSON: "{\"questions\":[{\"question\":\"Pick one\",\"options\":[\"A\",\"B\"]}]}")
        #expect(e.hookEventName == .askUserQuestion)
        #expect(e.source == "aimux")
        #expect(e.requestId == "req-q")
        #expect(e.sessionId == "s1")
        #expect(e.toolName == "AskUserQuestion")
        #expect(e.toolInputJSON == "{\"questions\":[{\"question\":\"Pick one\",\"options\":[\"A\",\"B\"]}]}")
    }

    @Test("an aimux event ingests into the store as a pending actionable card")
    func ingestsAsPendingCard() {
        let store = WorkstreamStore(ringCapacity: 10)
        store.ingest(AimuxWorkstreamTransport.permissionEvent(
            sessionId: "glyde", requestId: "req-9", toolName: "Bash",
            toolInputJSON: "{\"command\":\"ls\"}"))
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

    @Test("an aimux question event ingests into the store as a pending question card")
    func ingestsQuestionAsPendingCard() {
        let store = WorkstreamStore(ringCapacity: 10)
        store.ingest(AimuxWorkstreamTransport.questionEvent(
            sessionId: "glyde", requestId: "req-q",
            toolInputJSON: "{\"questions\":[{\"question\":\"Pick one\",\"options\":[\"A\",\"B\"]}]}"
        ))
        #expect(store.items.count == 1)
        #expect(store.pending.count == 1)
        let item = store.items[0]
        #expect(item.source == .aimux)
        #expect(item.kind == .question)
        if case .question(let rid, let questions) = item.payload {
            #expect(rid == "req-q")
            #expect(questions.first?.prompt == "Pick one")
            #expect(questions.first?.options.map(\.label) == ["A", "B"])
        } else {
            Issue.record("expected question payload")
        }
    }
}
