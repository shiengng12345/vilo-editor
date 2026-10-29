use std::time::Duration;

use gpui::{ClipboardItem, ScrollStrategy, Task, UniformListScrollHandle, Window, prelude::*, uniform_list};
use ui::{Button, ButtonStyle, Checkbox, ContextMenu, IconButton, Indicator, PopoverMenu, ToggleState, Tooltip, prelude::*};

use super::{ArgoView, Fetch, health_color};
use crate::argo::{ContainerState, Event, Resource, container_states};

const LOG_LINES: u32 = 1000;
/// How often a followed log is read again.
const FOLLOW_EVERY: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, PartialEq)]
pub(super) enum PodTab {
    Summary,
    Events,
    Logs,
    Manifest,
}

pub(super) struct PodPanel {
    pub(super) pod: Resource,
    tab: PodTab,
    manifest: Fetch<serde_json::Value>,
    /// The manifest as display lines, made once rather than on every frame.
    manifest_lines: Vec<SharedString>,
    containers: Vec<ContainerState>,
    events: Fetch<Vec<Event>>,
    logs: Fetch<Vec<SharedString>>,
    container: Option<String>,
    follow: bool,
    log_scroll: UniformListScrollHandle,
    manifest_scroll: UniformListScrollHandle,
    _loads: Vec<Task<()>>,
    _follow: Option<Task<()>>,
}

impl ArgoView {
    pub(super) fn open_pod(&mut self, pod: Resource, _window: &mut Window, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let Some(detail) = &mut self.detail else {
            return;
        };
        let (namespace, name) = (detail.namespace.clone(), detail.name.clone());
        let generation = self.generation;
        let uid = pod.uid.clone();
        let is_current = move |this: &ArgoView| {
            this.generation == generation
                && this.detail.as_ref().and_then(|detail| detail.pod.as_ref()).is_some_and(|panel| panel.pod.uid == uid)
        };
        let manifest_task = {
            let (client, namespace, name, pod, is_current) = (client.clone(), namespace.clone(), name.clone(), pod.clone(), is_current.clone());
            cx.spawn(async move |this, cx| {
                let result = client.pod_manifest(&namespace, &name, &pod).await;
                this.update(cx, |this, cx| {
                    if !is_current(this) {
                        return;
                    }
                    if let Some(panel) = this.detail.as_mut().and_then(|detail| detail.pod.as_mut()) {
                        match result {
                            Ok(manifest) => {
                                panel.containers = container_states(&manifest);
                                panel.manifest_lines = serde_json::to_string_pretty(&manifest)
                                    .unwrap_or_default()
                                    .lines()
                                    .map(|line| SharedString::from(line.to_string()))
                                    .collect();
                                panel.manifest = Fetch::Loaded(manifest);
                            }
                            Err(error) => panel.manifest = Fetch::Failed(error.to_string().into()),
                        }
                    }
                    cx.notify();
                })
                .ok();
            })
        };
        let events_task = {
            let pod = pod.clone();
            cx.spawn(async move |this, cx| {
                let result = client.resource_events(&namespace, &name, &pod).await;
                this.update(cx, |this, cx| {
                    if !is_current(this) {
                        return;
                    }
                    if let Some(panel) = this.detail.as_mut().and_then(|detail| detail.pod.as_mut()) {
                        panel.events = match result {
                            Ok(events) => Fetch::Loaded(events),
                            Err(error) => Fetch::Failed(error.to_string().into()),
                        };
                    }
                    cx.notify();
                })
                .ok();
            })
        };
        detail.pod = Some(PodPanel {
            pod,
            tab: PodTab::Summary,
            manifest: Fetch::Loading,
            manifest_lines: Vec::new(),
            containers: Vec::new(),
            events: Fetch::Loading,
            logs: Fetch::Loading,
            container: None,
            follow: false,
            log_scroll: UniformListScrollHandle::new(),
            manifest_scroll: UniformListScrollHandle::new(),
            _loads: vec![manifest_task, events_task],
            _follow: None,
        });
        self.load_logs(cx);
        cx.notify();
    }

    fn load_logs(&mut self, cx: &mut Context<Self>) {
        let Some(client) = self.session.as_ref().map(|session| session.client.clone()) else {
            return;
        };
        let Some(detail) = &mut self.detail else {
            return;
        };
        let (namespace, name) = (detail.namespace.clone(), detail.name.clone());
        let Some(panel) = &mut detail.pod else {
            return;
        };
        let (pod, container) = (panel.pod.clone(), panel.container.clone());
        let generation = self.generation;
        let task = cx.spawn(async move |this, cx| {
            let result = client.pod_logs(&namespace, &name, &pod, container.as_deref(), LOG_LINES).await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(panel) = this.detail.as_mut().and_then(|detail| detail.pod.as_mut()).filter(|panel| panel.pod.uid == pod.uid && panel.container == container) else {
                    return;
                };
                panel.logs = match result {
                    Ok(lines) => Fetch::Loaded(lines.iter().map(|line| SharedString::from(crate::jenkins::strip_escapes(line))).collect()),
                    Err(error) => Fetch::Failed(error.to_string().into()),
                };
                if panel.follow
                    && let Fetch::Loaded(lines) = &panel.logs
                    && !lines.is_empty()
                {
                    panel.log_scroll.scroll_to_item(lines.len() - 1, ScrollStrategy::Bottom);
                }
                cx.notify();
            })
            .ok();
        });
        panel._loads.push(task);
        // Only the latest few loads matter; older ones finished or are superseded.
        if panel._loads.len() > 4 {
            panel._loads.drain(2..panel._loads.len() - 1);
        }
    }

    fn set_follow(&mut self, follow: bool, cx: &mut Context<Self>) {
        let Some(panel) = self.detail.as_mut().and_then(|detail| detail.pod.as_mut()) else {
            return;
        };
        panel.follow = follow;
        panel._follow = follow.then(|| {
            cx.spawn(async move |this, cx| {
                loop {
                    cx.background_executor().timer(FOLLOW_EVERY).await;
                    // Paused while the page is out of view; ends when the pod closes.
                    let keep = this
                        .update(cx, |this, cx| {
                            let open = this.detail.as_ref().and_then(|detail| detail.pod.as_ref()).is_some_and(|panel| panel.follow);
                            if open && this.visible {
                                this.load_logs(cx);
                            }
                            open
                        })
                        .unwrap_or(false);
                    if !keep {
                        return;
                    }
                }
            })
        });
        if follow {
            self.load_logs(cx);
        }
        cx.notify();
    }

    pub(super) fn render_pod(&self, panel: &PodPanel, cx: &mut Context<Self>) -> AnyElement {
        let app = self.detail.as_ref().map(|detail| detail.name.clone()).unwrap_or_default();
        let tab_button = |id: &'static str, label: &'static str, tab: PodTab, cx: &mut Context<Self>| {
            Button::new(id, label)
                .label_size(LabelSize::Small)
                .style(if panel.tab == tab { ButtonStyle::Filled } else { ButtonStyle::Subtle })
                .on_click(cx.listener(move |this, _, _, cx| {
                    if let Some(panel) = this.detail.as_mut().and_then(|detail| detail.pod.as_mut()) {
                        panel.tab = tab;
                    }
                    cx.notify();
                }))
        };
        let header = v_flex()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Button::new("argo-pod-back", app)
                            .label_size(LabelSize::Small)
                            .start_icon(Icon::new(IconName::ArrowLeft).size(IconSize::Small))
                            .on_click(cx.listener(|this, _, _, cx| {
                                if let Some(detail) = &mut this.detail {
                                    detail.pod = None;
                                }
                                cx.notify();
                            })),
                    )
                    .child(Indicator::dot().color(panel.pod.health.as_deref().map_or(Color::Muted, health_color)))
                    .child(Label::new(panel.pod.name.clone()).size(LabelSize::Large).buffer_font(cx).truncate()),
            )
            .child(
                h_flex()
                    .gap_1()
                    .child(tab_button("argo-pod-summary", "Summary", PodTab::Summary, cx))
                    .child(tab_button("argo-pod-events", "Events", PodTab::Events, cx))
                    .child(tab_button("argo-pod-logs", "Logs", PodTab::Logs, cx))
                    .child(tab_button("argo-pod-manifest", "Manifest", PodTab::Manifest, cx)),
            );
        let body = match panel.tab {
            PodTab::Summary => self.render_pod_summary(panel, cx),
            PodTab::Events => render_pod_events(panel, cx),
            PodTab::Logs => self.render_pod_logs(panel, cx),
            PodTab::Manifest => match &panel.manifest {
                Fetch::Loading => Label::new("Loading…").size(LabelSize::Small).color(Color::Muted).into_any_element(),
                Fetch::Failed(message) => Label::new(message.clone()).size(LabelSize::Small).color(Color::Warning).into_any_element(),
                Fetch::Loaded(_) => render_lines("argo-pod-manifest-lines", panel.manifest_lines.clone(), &panel.manifest_scroll, cx),
            },
        };
        v_flex().flex_1().min_h_0().gap_3().child(header).child(body).into_any_element()
    }

    fn render_pod_summary(&self, panel: &PodPanel, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let row = |label: &'static str, value: String, cx: &mut Context<Self>| {
            h_flex()
                .px_3()
                .py_1p5()
                .gap_3()
                .border_t_1()
                .border_color(colors.border_variant)
                .child(div().w(px(160.)).flex_none().child(Label::new(label).size(LabelSize::Small).color(Color::Muted)))
                .child(div().flex_1().min_w_0().child(Label::new(value).size(LabelSize::Small).buffer_font(cx)))
        };
        let pod = &panel.pod;
        let mut table = v_flex()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .child(row("NAME", pod.name.clone(), cx))
            .child(row("NAMESPACE", pod.namespace.clone(), cx))
            .child(row("CREATED", pod.created_at.clone().unwrap_or_default().replace('T', " ").trim_end_matches('Z').to_string(), cx))
            .child(row("STATE", pod.info("Status Reason").unwrap_or("").to_string(), cx))
            .child(row("HEALTH", pod.health.clone().unwrap_or_else(|| "Unknown".into()), cx));
        if let Some(node) = pod.info("Node") {
            table = table.child(row("NODE", node.to_string(), cx));
        }
        for image in &pod.images {
            table = table.child(row("IMAGE", image.clone(), cx));
        }
        let containers = match &panel.manifest {
            Fetch::Loading => Label::new("Loading containers…").size(LabelSize::Small).color(Color::Muted).into_any_element(),
            Fetch::Failed(message) => Label::new(message.clone()).size(LabelSize::Small).color(Color::Warning).into_any_element(),
            Fetch::Loaded(_) => v_flex()
                .rounded_md()
                .border_1()
                .border_color(colors.border)
                .children(panel.containers.iter().map(|container| {
                    let healthy = container.ready && container.state == "running";
                    v_flex()
                        .px_3()
                        .py_1p5()
                        .gap_0p5()
                        .border_t_1()
                        .border_color(colors.border_variant)
                        .child(
                            h_flex()
                                .gap_2()
                                .child(Indicator::dot().color(if healthy { Color::Success } else { Color::Error }))
                                .child(Label::new(container.name.clone()).size(LabelSize::Small).weight(gpui::FontWeight::SEMIBOLD))
                                .child(Label::new(container.state.clone()).size(LabelSize::Small).color(if healthy { Color::Default } else { Color::Error }))
                                .child(Label::new(if container.ready { "ready" } else { "not ready" }).size(LabelSize::XSmall).color(Color::Muted))
                                .child(Label::new(format!("{} restarts", container.restarts)).size(LabelSize::XSmall).color(Color::Muted)),
                        )
                        .child(Label::new(container.image.clone()).size(LabelSize::XSmall).buffer_font(cx).color(Color::Muted).truncate())
                        .when_some(container.message.clone(), |this, message| {
                            this.child(Label::new(message).size(LabelSize::XSmall).color(Color::Warning))
                        })
                }))
                .into_any_element(),
        };
        v_flex()
            .id("argo-pod-summary-body")
            .flex_1()
            .min_h_0()
            .overflow_y_scroll()
            .gap_3()
            .child(table)
            .child(Label::new("Containers").size(LabelSize::Small).weight(gpui::FontWeight::SEMIBOLD))
            .child(containers)
            .into_any_element()
    }

    fn render_pod_logs(&self, panel: &PodPanel, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let query = self.log_filter.read(cx).text(cx);
        let (lines, total) = match &panel.logs {
            Fetch::Loaded(lines) if query.is_empty() => (lines.clone(), lines.len()),
            Fetch::Loaded(lines) => (lines.iter().filter(|line| line.contains(query.as_str())).cloned().collect(), lines.len()),
            _ => (Vec::new(), 0),
        };
        let names = panel.containers.iter().map(|container| container.name.clone()).collect::<Vec<_>>();
        let current = panel.container.clone().or_else(|| names.first().cloned()).unwrap_or_else(|| "default container".into());
        let view = cx.entity().downgrade();
        let copy = lines.iter().map(|line| line.to_string()).collect::<Vec<_>>().join("\n");
        let toolbar = h_flex()
            .gap_2()
            .when(names.len() > 1, |this| {
                this.child(
                    PopoverMenu::new("argo-log-container")
                        .trigger(
                            Button::new("argo-log-container-button", current.clone())
                                .label_size(LabelSize::Small)
                                .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
                        )
                        .menu(move |window, cx| {
                            let names = names.clone();
                            let view = view.clone();
                            Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                                for name in &names {
                                    let view = view.clone();
                                    let name = name.clone();
                                    menu = menu.entry(name.clone(), None, move |_, cx| {
                                        view.update(cx, |this, cx| {
                                            if let Some(panel) = this.detail.as_mut().and_then(|detail| detail.pod.as_mut()) {
                                                panel.container = Some(name.clone());
                                                panel.logs = Fetch::Loading;
                                            }
                                            this.load_logs(cx);
                                        })
                                        .ok();
                                    });
                                }
                                menu
                            }))
                        }),
                )
            })
            .child(
                div()
                    .w(px(280.))
                    .px_2()
                    .py_0p5()
                    .rounded_sm()
                    .border_1()
                    .border_color(colors.border)
                    .bg(colors.editor_background)
                    .child(self.log_filter.clone()),
            )
            .child(
                Checkbox::new("argo-log-follow", ToggleState::from(panel.follow))
                    .label("Follow")
                    .label_size(LabelSize::Small)
                    .on_click(cx.listener(|this, state: &ToggleState, _, cx| this.set_follow(state.selected(), cx))),
            )
            .child(div().flex_1())
            .child(
                Label::new(if query.is_empty() {
                    format!("last {total} lines")
                } else {
                    format!("{} of {total} lines", lines.len())
                })
                .size(LabelSize::XSmall)
                .color(Color::Muted),
            )
            .child(
                IconButton::new("argo-log-reload", IconName::RotateCw)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Read the latest lines again"))
                    .on_click(cx.listener(|this, _, _, cx| this.load_logs(cx))),
            )
            .child(
                IconButton::new("argo-log-copy", IconName::Copy)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Copy the lines shown"))
                    .on_click(move |_, _, cx| cx.write_to_clipboard(ClipboardItem::new_string(copy.clone()))),
            );
        let body = match &panel.logs {
            Fetch::Loading => Label::new("Loading…").size(LabelSize::Small).color(Color::Muted).into_any_element(),
            Fetch::Failed(message) => Label::new(message.clone()).size(LabelSize::Small).color(Color::Warning).into_any_element(),
            Fetch::Loaded(_) if lines.is_empty() => Label::new(if query.is_empty() { "No log lines." } else { "No lines match." })
                .size(LabelSize::Small)
                .color(Color::Muted)
                .into_any_element(),
            Fetch::Loaded(_) => render_lines("argo-pod-log-lines", lines, &panel.log_scroll, cx),
        };
        v_flex().flex_1().min_h_0().gap_2().child(toolbar).child(body).into_any_element()
    }
}

fn render_pod_events(panel: &PodPanel, cx: &App) -> AnyElement {
    let colors = cx.theme().colors();
    match &panel.events {
        Fetch::Loading => Label::new("Loading…").size(LabelSize::Small).color(Color::Muted).into_any_element(),
        Fetch::Failed(message) => Label::new(message.clone()).size(LabelSize::Small).color(Color::Warning).into_any_element(),
        Fetch::Loaded(events) if events.is_empty() => {
            Label::new("No events for this pod.").size(LabelSize::Small).color(Color::Muted).into_any_element()
        }
        Fetch::Loaded(events) => v_flex()
            .id("argo-pod-events-body")
            .flex_1()
            .min_h_0()
            .overflow_y_scroll()
            .rounded_md()
            .border_1()
            .border_color(colors.border)
            .child(
                h_flex()
                    .px_3()
                    .py_1()
                    .gap_3()
                    .bg(colors.element_background)
                    .child(div().w(px(140.)).flex_none().child(Label::new("REASON").size(LabelSize::XSmall).color(Color::Muted)))
                    .child(div().flex_1().child(Label::new("MESSAGE").size(LabelSize::XSmall).color(Color::Muted)))
                    .child(div().w(px(50.)).flex_none().child(Label::new("COUNT").size(LabelSize::XSmall).color(Color::Muted)))
                    .child(div().w(px(150.)).flex_none().child(Label::new("LAST").size(LabelSize::XSmall).color(Color::Muted))),
            )
            .children(events.iter().map(|event| {
                h_flex()
                    .px_3()
                    .py_1p5()
                    .gap_3()
                    .items_start()
                    .border_t_1()
                    .border_color(colors.border_variant)
                    .child(div().w(px(140.)).flex_none().child(Label::new(event.reason.clone()).size(LabelSize::Small).color(
                        if event.kind == "Warning" { Color::Warning } else { Color::Default },
                    )))
                    .child(div().flex_1().min_w_0().child(Label::new(event.message.clone()).size(LabelSize::Small)))
                    .child(div().w(px(50.)).flex_none().child(Label::new(event.count.to_string()).size(LabelSize::Small)))
                    .child(div().w(px(150.)).flex_none().child(
                        Label::new(event.last_seen.replace('T', " ").trim_end_matches('Z').to_string()).size(LabelSize::XSmall).color(Color::Muted),
                    ))
            }))
            .into_any_element(),
    }
}

/// A long text shown as a virtual list: only the visible lines are laid out.
/// The lines are shared strings, so handing them over each frame copies pointers only.
fn render_lines(id: &'static str, lines: Vec<SharedString>, scroll: &UniformListScrollHandle, cx: &mut Context<ArgoView>) -> AnyElement {
    let colors = cx.theme().colors().clone();
    let count = lines.len();
    let list = uniform_list(id, count, move |range, _, cx| {
        range
            .filter_map(|index| lines.get(index).cloned())
            .map(|line| div().px_2().child(Label::new(line).size(LabelSize::XSmall).buffer_font(cx).truncate()).into_any_element())
            .collect::<Vec<_>>()
    })
    .track_scroll(scroll)
    .flex_1();
    v_flex()
        .flex_1()
        .min_h_0()
        .py_1()
        .rounded_md()
        .border_1()
        .border_color(colors.border)
        .bg(colors.editor_subheader_background)
        .child(list)
        .into_any_element()
}
