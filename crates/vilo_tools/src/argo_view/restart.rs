use std::io::Write as _;

use editor::Editor;
use gpui::{Entity, Task, Window, prelude::*};
use ui::{Button, ButtonStyle, prelude::*};

use super::{ArgoView, editor};
use crate::argo::{Resource, WriteError};

/// Kinds Argo's built-in "restart" action supports.
pub(super) fn can_restart(resource: &Resource) -> bool {
    matches!(resource.kind.as_str(), "Deployment" | "StatefulSet" | "DaemonSet" | "Rollout")
}

enum Step {
    Checking,
    Unavailable(SharedString),
    Ready(Option<SharedString>),
    Running,
    Finished(Result<(), WriteError>),
}

/// A restart being confirmed or sent, bound to one workload of the open application.
pub(super) struct RestartPanel {
    workload: Resource,
    step: Step,
    /// Asked for on production connections: the workload's name, typed.
    typed: Option<Entity<Editor>>,
    _task: Option<Task<()>>,
}

impl RestartPanel {
    pub(super) fn is_for(&self, workload: &Resource) -> bool {
        self.workload.uid == workload.uid
    }
}

impl ArgoView {
    pub(super) fn is_production(&self) -> bool {
        self.current_connection().is_some_and(|connection| {
            let text = format!("{} {} {}", connection.environment, connection.name, connection.address).to_lowercase();
            text.contains("prod") || text.contains("live")
        })
    }

    pub(super) fn start_restart(&mut self, workload: Resource, window: &mut Window, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let typed = self.is_production().then(|| editor(&workload.name, "", false, window, cx));
        let Some(detail) = &mut self.detail else {
            return;
        };
        if detail.restart.as_ref().is_some_and(|panel| matches!(panel.step, Step::Running)) {
            return;
        }
        let (namespace, name, generation) = (detail.namespace.clone(), detail.name.clone(), self.generation);
        let task = {
            let workload = workload.clone();
            cx.spawn(async move |this, cx| {
                let actions = client.resource_actions(&namespace, &name, &workload).await;
                this.update(cx, |this, cx| {
                    let Some(panel) = this.restart_panel(generation, &namespace, &name, &workload) else {
                        return;
                    };
                    panel.step = match actions {
                        Err(error) => Step::Unavailable(format!("Could not ask Argo CD which actions it allows: {error}").into()),
                        Ok(actions) => match actions.iter().find(|action| action.name == "restart") {
                            None => Step::Unavailable("Argo CD offers no restart for this workload.".into()),
                            Some(action) if action.disabled => Step::Unavailable("Argo CD has restart turned off for this workload right now.".into()),
                            Some(action) if action.has_params => Step::Unavailable("This restart asks for parameters Vilo does not support.".into()),
                            Some(_) => Step::Ready(None),
                        },
                    };
                    cx.notify();
                })
                .ok();
            })
        };
        detail.restart = Some(RestartPanel { workload, step: Step::Checking, typed, _task: Some(task) });
        cx.notify();
    }

    fn restart_panel(&mut self, generation: u64, namespace: &str, name: &str, workload: &Resource) -> Option<&mut RestartPanel> {
        if self.generation != generation {
            return None;
        }
        self.detail
            .as_mut()
            .filter(|detail| detail.namespace == namespace && detail.name == name)
            .and_then(|detail| detail.restart.as_mut())
            .filter(|panel| panel.is_for(workload))
    }

    fn confirm_restart(&mut self, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let connection = self.current_connection().map(|connection| (connection.name.clone(), connection.address.clone(), connection.username.clone()));
        let Some(detail) = &mut self.detail else {
            return;
        };
        let (namespace, name, generation) = (detail.namespace.clone(), detail.name.clone(), self.generation);
        let Some(panel) = &mut detail.restart else {
            return;
        };
        if !matches!(panel.step, Step::Ready(_)) {
            return;
        }
        if let Some(typed) = &panel.typed
            && typed.read(cx).text(cx).trim() != panel.workload.name
        {
            panel.step = Step::Ready(Some(format!("Type “{}” to confirm.", panel.workload.name).into()));
            cx.notify();
            return;
        }
        panel.step = Step::Running;
        let workload = panel.workload.clone();
        panel._task = Some(cx.spawn(async move |this, cx| {
            let result = client.run_action(&namespace, &name, &workload, "restart").await;
            this.update(cx, |this, cx| {
                let (connection, address, user) = connection.unwrap_or_default();
                append_audit(
                    serde_json::json!({
                        "time": time::OffsetDateTime::now_utc().unix_timestamp(),
                        "connection": connection,
                        "address": address,
                        "user": user,
                        "app_namespace": namespace,
                        "app": name,
                        "kind": workload.kind,
                        "namespace": workload.namespace,
                        "name": workload.name,
                        "action": "restart",
                        "outcome": match &result {
                            Ok(()) => "accepted",
                            Err(WriteError::Rejected(_)) | Err(WriteError::SignInAgain) => "refused",
                            Err(WriteError::Unknown(_)) => "unknown",
                        },
                    }),
                    cx,
                );
                if let Some(panel) = this.restart_panel(generation, &namespace, &name, &workload) {
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

    pub(super) fn render_restart(&self, panel: &RestartPanel, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let workload = &panel.workload;
        let close = |id: &'static str, label: &'static str, cx: &mut Context<Self>| {
            Button::new(id, label).label_size(LabelSize::Small).on_click(cx.listener(|this, _, _, cx| {
                if let Some(detail) = &mut this.detail {
                    detail.restart = None;
                }
                cx.notify();
            }))
        };
        let mut content = v_flex().gap_1p5();
        match &panel.step {
            Step::Checking => {
                content = content.child(Label::new("Checking what Argo CD allows…").size(LabelSize::Small).color(Color::Muted));
            }
            Step::Unavailable(reason) => {
                content = content
                    .child(Label::new(reason.clone()).size(LabelSize::Small).color(Color::Warning))
                    .child(h_flex().child(close("argo-restart-close", "Close", cx)));
            }
            Step::Ready(message) => {
                content = content
                    .child(
                        Label::new(format!("Restart {} “{}” in {}?", workload.kind, workload.name, workload.namespace))
                            .size(LabelSize::Small)
                            .weight(gpui::FontWeight::SEMIBOLD),
                    )
                    .child(
                        Label::new("Argo CD updates the pod template, so the pods are replaced under the workload's rollout strategy. Requests may fail while pods start.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .when_some(panel.typed.clone(), |this, typed| {
                        this.child(Label::new(format!("Production: type “{}” to confirm.", workload.name)).size(LabelSize::Small).color(Color::Warning))
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
                                Button::new("argo-restart-confirm", "Restart")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Filled)
                                    .on_click(cx.listener(|this, _, _, cx| this.confirm_restart(cx))),
                            )
                            .child(close("argo-restart-cancel", "Cancel", cx)),
                    );
            }
            Step::Running => {
                content = content.child(Label::new(format!("Restarting {}…", workload.name)).size(LabelSize::Small).color(Color::Muted));
            }
            Step::Finished(result) => {
                let (message, color) = match result {
                    Ok(()) => ("Restart requested. New pods appear here within a few seconds.".to_string(), Color::Success),
                    Err(error) => (error.to_string(), Color::Error),
                };
                content = content
                    .child(Label::new(message).size(LabelSize::Small).color(color))
                    .child(h_flex().child(close("argo-restart-done", "Close", cx)));
            }
        }
        div()
            .pl_8()
            .pr_3()
            .py_2()
            .border_t_1()
            .border_color(colors.border_variant)
            .bg(colors.element_background)
            .child(content)
            .into_any_element()
    }
}

pub(super) fn append_audit(line: serde_json::Value, cx: &mut App) {
    let line = line.to_string();
    cx.background_spawn(async move {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths::config_dir().join("argo_audit.log"))?;
        writeln!(file, "{line}")
    })
    .detach_and_log_err(cx);
}
