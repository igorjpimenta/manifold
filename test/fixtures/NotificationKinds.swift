import Foundation
import UserNotifications

// NotificationKinds — single source of truth for user-facing notification
// surfaces the iOS app can receive (via APNs in production, via local
// scheduling only during dev). Mirrors extensions/live-activity-bridge/
// notifications.js — the `aps.category` string is the cross-process key,
// and `userInfo` schemas must match field-for-field.
//
// Adding a new surface = adding a case here AND an entry in the
// server-side KINDS table. Forgetting either side means the
// notification delivers but doesn't navigate.

@available(iOS 17.0, *)
enum NotificationKind: String, CaseIterable {
    case approval = "openclaw.approval"
    case failure  = "openclaw.failure"
    case budget   = "openclaw.budget"
    case digest   = "openclaw.digest"
    case deploy   = "openclaw.deploy"

    /// Identifier used by the system when registering the action category.
    var categoryIdentifier: String { rawValue }

    /// Per-kind action(s) — the labels the user sees when long-pressing
    /// the banner. Keep these terse and aligned with the in-app verb.
    var actions: [UNNotificationAction] {
        switch self {
        case .approval:
            return [UNNotificationAction(
                identifier: "openclaw.approval.open",
                title: "Open",
                options: [.foreground]
            )]
        case .failure:
            return [UNNotificationAction(
                identifier: "openclaw.failure.triage",
                title: "Triage",
                options: [.foreground]
            )]
        case .budget:
            return [UNNotificationAction(
                identifier: "openclaw.budget.review",
                title: "Review",
                options: [.foreground]
            )]
        case .digest:
            // Digests carry title + body only — no ids, so there is nothing
            // to route deeper than the operator's dashboard. A per-kind
            // destination would need the server to emit a discriminator it
            // doesn't currently send.
            return [UNNotificationAction(
                identifier: "openclaw.digest.open",
                title: "Open",
                options: [.foreground]
            )]
        case .deploy:
            // Opens the workflow's run list on GitHub, which already offers
            // logs and re-run. Deliberately not an in-app destination yet —
            // there is no Deploys screen to land on.
            return [UNNotificationAction(
                identifier: "openclaw.deploy.openRun",
                title: "Open run",
                options: [.foreground]
            )]
        }
    }

    /// Build the UNNotificationCategory the system needs for action
    /// rendering + delegate dispatch.
    var category: UNNotificationCategory {
        UNNotificationCategory(
            identifier: categoryIdentifier,
            actions: actions,
            intentIdentifiers: [],
            options: []
        )
    }

    /// Resolve the userInfo dict iOS receives (from APNs or local) into
    /// a strongly-typed routing payload. Returns nil if the payload
    /// doesn't match the expected schema — the caller should drop the
    /// tap silently in that case (server/client schema drift).
    static func routing(from userInfo: [AnyHashable: Any]) -> NotificationRouting? {
        // Prefer the explicit `kind` field (server emits it) for
        // forward-compat; fall back to `aps.category` so a stray hand-
        // crafted payload still routes.
        let kindRaw = (userInfo["kind"] as? String)
            ?? ((userInfo["aps"] as? [String: Any])?["category"] as? String)
        guard let kindRaw, let kind = NotificationKind(rawValue: kindRaw) else { return nil }
        switch kind {
        case .approval:
            guard let flowId = userInfo["flowId"] as? String, !flowId.isEmpty else { return nil }
            return .approval(
                flowId: flowId,
                itemId: userInfo["itemId"] as? String
            )
        case .failure:
            guard let failureId = userInfo["failureId"] as? String, !failureId.isEmpty else { return nil }
            return .failure(
                failureId: failureId,
                flowId: userInfo["flowId"] as? String
            )
        case .budget:
            return .budget
        case .digest:
            return .digest
        case .deploy:
            // runUrl is the only required field — without somewhere to send
            // the tap this notification has no action worth taking, so drop
            // it rather than open the app to an arbitrary screen.
            guard let runUrlRaw = userInfo["runUrl"] as? String,
                  let runUrl = URL(string: runUrlRaw) else { return nil }
            return .deploy(
                runUrl: runUrl,
                mode: userInfo["mode"] as? String,
                sha: userInfo["sha"] as? String
            )
        }
    }
}

@available(iOS 17.0, *)
enum NotificationRouting {
    case approval(flowId: String, itemId: String?)
    case failure(failureId: String, flowId: String?)
    case budget
    case digest
    case deploy(runUrl: URL, mode: String?, sha: String?)
}
