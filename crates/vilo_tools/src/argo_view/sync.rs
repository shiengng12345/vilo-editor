use editor::Editor;
use gpui::{Entity, Window, prelude::*};
use ui::{Button, ButtonStyle, Checkbox, ToggleState, prelude::*};

use super::{
    ArgoView, Fetch, editor,
    restart::{IN_FLIGHT, append_audit, write_key},
};
use crate::argo::{AppDetail, WriteError};

#[derive(Clone, Copy, PartialEq)]
pub(super) enum AppAction {
    Sync { prune: bool },
    /// Turns Argo's automated sync on (`true`) or off.
    AutoSync(bool),
}

enum Step {
    Ready(Option<SharedString>),
    Running,
    Finished(Result<(), WriteError>),
}

/// A sync or auto-sync change being confirmed or sent for the open application.
pub(super) struct AppActionPanel {
    action: AppAction,
    step: Step,
    /// Asked for on production connections: the application's name, typed.
    typed: Option<Entity<Editor>>,
}

impl ArgoView {
    pub(super) fn start_app_action(&mut self, action: AppAction, window: &mut Window, cx: &mut Context<Self>) {
        let typed = self.is_production().then(|| {
            let name = self.detail.as_ref().map(|detail| detail.name.clone()).unwrap_or_default();
            editor(&name, "", false, window, cx)
        });
        let Some(detail) = &mut self.detail else {
            return;
        };
        if detail.app_action.as_ref().is_some_and(|panel| matches!(panel.step, Step::Running)) {
            return;
        }
        detail.app_action = Some(AppActionPanel { action, step: Step::Ready(None), typed });
        cx.notify();
    }

    fn confirm_app_action(&mut self, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let connection = self.current_connection().map(|connection| (connection.name.clone(), connection.address.clone(), connection.username.clone()));
        let generation = self.generation;
        let Some(detail) = &mut self.detail else {
            return;
        };
        let (namespace, name) = (detail.namespace.clone(), detail.name.clone());
        let Fetch::Loaded(app) = &detail.app else {
            return;
        };
        let (project, running) = (app.summary.project.clone(), app.summary.operation.as_deref() == Some("Running"));
        let Some(panel) = &mut detail.app_action else {
            return;
        };
        if !matches!(panel.step, Step::Ready(_)) {
            return;
        }
        // The poll may have seen an operation start since the panel opened.
        if running && matches!(panel.action, AppAction::Sync { .. }) {
            panel.step = Step::Ready(Some("An operation is already running. Wait for it to finish.".into()));
            cx.notify();
            return;
        }
        if let Some(typed) = &panel.typed
            && typed.read(cx).text(cx).trim() != name
        {
            panel.step = Step::Ready(Some(format!("Type “{name}” to confirm.").into()));
            cx.notify();
            return;
        }
        // One application-level write at a time: a sync and an auto-sync change share the key.
        let key = write_key(&self.session, &namespace, &name, "");
        if self.writes_in_flight.contains(&key) {
            panel.step = Step::Ready(Some(IN_FLIGHT.into()));
            cx.notify();
            return;
        }
        panel.step = Step::Running;
        let action = panel.action;
        self.writes_in_flight.insert(key.clone());
        self.tasks.push(cx.spawn(async move |this, cx| {
            let result = match action {
                AppAction::Sync { prune } => client.sync(&namespace, &name, &project, prune).await,
                AppAction::AutoSync(enabled) => client.set_auto_sync(&namespace, &name, &project, enabled).await,
            };
            this.update(cx, |this, cx| {
                this.writes_in_flight.remove(&key);
                let (connection, address, user) = connection.unwrap_or_default();
                append_audit(
                    serde_json::json!({
                        "time": time::OffsetDateTime::now_utc().unix_timestamp(),
                        "connection": connection,
                        "address": address,
                        "user": user,
                        "app_namespace": namespace,
                        "app": name,
                        "action": match action {
                            AppAction::Sync { prune: true } => "sync with prune",
                            AppAction::Sync { prune: false } => "sync",
                            AppAction::AutoSync(true) => "auto-sync on",
                            AppAction::AutoSync(false) => "auto-sync off",
                        },
                        "outcome": match &result {
                            Ok(()) => "accepted",
                            Err(WriteError::Rejected(_)) | Err(WriteError::SignInAgain) => "refused",
                            Err(WriteError::Unknown(_)) => "unknown",
                        },
                    }),
                    cx,
                );
                if this.generation == generation
                    && let Some(panel) = this
                        .detail
                        .as_mut()
                        .filter(|detail| detail.namespace == namespace && detail.name == name)
                        .and_then(|detail| detail.app_action.as_mut())
                        .filter(|panel| panel.action == action)
                {
                    panel.step = match result {
                        // A fresh session is ready; the user presses again rather than Vilo resending.
                        Err(WriteError::SignInAgain) => Step::Ready(Some(WriteError::SignInAgain.to_string().into())),
                        result => Step::Finished(result),
                    };
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }

    pub(super) fn render_app_action(&self, panel: &AppActionPanel, app: &AppDetail, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let close = |id: &'static str, label: &'static str, cx: &mut Context<Self>| {
            Button::new(id, label).label_size(LabelSize::Small).on_click(cx.listener(|this, _, _, cx| {
                if let Some(detail) = &mut this.detail {
                    detail.app_action = None;
                }
                cx.notify();
            }))
        };
        let note = |text: String| Label::new(text).size(LabelSize::Small).color(Color::Muted);
        let warn = |text: String| Label::new(text).size(LabelSize::Small).color(Color::Warning);
        let name = &app.summary.name;
        let mut content = v_flex().gap_1p5();
        match &panel.step {
            Step::Ready(message) => {
                let (title, confirm) = match panel.action {
                    AppAction::Sync { .. } => (format!("Sync “{name}”?"), "Sync"),
                    AppAction::AutoSync(true) => (format!("Turn on auto-sync for “{name}”?"), "Turn on"),
                    AppAction::AutoSync(false) => (format!("Turn off auto-sync for “{name}”?"), "Turn off"),
                };
                content = content.child(Label::new(title).size(LabelSize::Small).weight(gpui::FontWeight::SEMIBOLD));
                for source in &app.sources {
                    content = content.child(
                        Label::new(format!("{} · {} · {}", source.repo, source.path, source.target_revision))
                            .size(LabelSize::Small)
                            .buffer_font(cx)
                            .color(Color::Muted),
                    );
                }
                match panel.action {
                    AppAction::Sync { prune } => {
                        content = content
                            .child(note(format!(
                                "{} resources out of sync. Deploys each source's target revision above, as it is in Git now.",
                                app.out_of_sync
                            )))
                            .child(note("Sync options set on the app (for example Replace or Force) still apply and can recreate resources.".into()))
                            .when(app.auto_sync.is_some(), |this| this.child(note("This app auto-syncs, so Argo may already be applying it.".into())))
                            .child(
                                Checkbox::new("argo-sync-prune", ToggleState::from(prune))
                                    .label("Prune")
                                    .label_size(LabelSize::Small)
                                    .on_click(cx.listener(|this, state: &ToggleState, _, cx| {
                                        if let Some(panel) = this.detail.as_mut().and_then(|detail| detail.app_action.as_mut()) {
                                            panel.action = AppAction::Sync { prune: state.selected() };
                                        }
                                        cx.notify();
                                    })),
                            )
                            .when(prune, |this| this.child(warn("Prune deletes live resources that are no longer in Git.".into())));
                    }
                    AppAction::AutoSync(true) => {
                        content = content.child(note(
                            "Argo then deploys every new commit on the target revisions by itself, around the clock, even when Vilo is closed.".into(),
                        ));
                        content = content.child(note("Prune and self-heal stay as the app has them (off unless set before).".into()));
                    }
                    AppAction::AutoSync(false) => {
                        content = content.child(note("Argo stops deploying new commits by itself. Manual Sync keeps working.".into()));
                    }
                }
                if matches!(panel.action, AppAction::AutoSync(_))
                    && let Some(parent) = &app.managed_by
                {
                    content = content.child(warn(format!(
                        "This app's definition comes from Git through “{parent}”. Argo may switch it back on the next sync of “{parent}”; change it in Git to make it stick."
                    )));
                }
                content = content
                    .when_some(panel.typed.clone(), |this, typed| {
                        this.child(warn(format!("Production: type “{name}” to confirm.")))
                            .child(
                                div()
                                    .w(px(320.))
                                    .px_2()
                                    .py_0p5()
                                    .rounded_sm()
                                    .border_1()
                                    .border_color(colors.border)
                                    .bg(colors.editor_background)
                                    .child(typed),
                            )
                    })
                    .when_some(message.clone(), |this, message| this.child(Label::new(message).size(LabelSize::Small).color(Color::Warning)))
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                Button::new("argo-app-action-confirm", confirm)
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Filled)
                                    .on_click(cx.listener(|this, _, _, cx| this.confirm_app_action(cx))),
                            )
                            .child(close("argo-app-action-cancel", "Cancel", cx)),
                    );
            }
            Step::Running => content = content.child(note("Sending to Argo CD…".into())),
            Step::Finished(result) => {
                let (message, color) = match (result, panel.action) {
                    (Ok(()), AppAction::Sync { .. }) => ("Sync started. Progress shows above as the last operation.".to_string(), Color::Success),
                    (Ok(()), AppAction::AutoSync(_)) => ("Saved. The page shows the new setting within a few seconds.".to_string(), Color::Success),
                    (Err(error), _) => (error.to_string(), Color::Error),
                };
                content = content
                    .child(Label::new(message).size(LabelSize::Small).color(color))
                    .child(h_flex().child(close("argo-app-action-done", "Close", cx)));
            }
        }
        div()
            .px_3()
            .py_2()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .bg(colors.element_background)
            .child(content)
            .into_any_element()
    }
}
