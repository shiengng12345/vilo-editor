use gpui::{Context, Entity, Subscription, Window};
use project::{
    Project,
    git_store::{GitStore, GitStoreEvent},
};
use ui::{ButtonLike, Tooltip, prelude::*};
use workspace::{HideStatusItem, ItemHandle, StatusItemView, Workspace};

/// Vilo: VS Code's branch indicator at the left of the status bar, e.g. `ap-master*`.
pub struct BranchStatusItem {
    project: Entity<Project>,
    _subscription: Subscription,
}

impl BranchStatusItem {
    pub fn new(workspace: &Workspace, cx: &mut Context<Self>) -> Self {
        let project = workspace.project().clone();
        let git_store = project.read(cx).git_store().clone();
        let subscription = cx.subscribe(
            &git_store,
            |_, _: Entity<GitStore>, _: &GitStoreEvent, cx| cx.notify(),
        );
        Self {
            project,
            _subscription: subscription,
        }
    }
}

impl Render for BranchStatusItem {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(repository) = self.project.read(cx).active_repository(cx) else {
            return div().into_any_element();
        };
        let repository = repository.read(cx);
        let Some(name) = repository
            .branch
            .as_ref()
            .map(|branch| branch.name().to_string())
            .or_else(|| {
                repository
                    .head_commit
                    .as_ref()
                    .map(|commit| commit.sha.chars().take(8).collect())
            })
        else {
            return div().into_any_element();
        };
        let status = repository.status_summary();
        // VS Code marks uncommitted changes with a trailing `*`.
        let dirty = status.count > 0;
        let label = if dirty { format!("{name}*") } else { name };

        ButtonLike::new("status-bar-branch")
            .child(
                h_flex()
                    .gap_1()
                    .child(
                        Icon::new(IconName::GitBranch)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(Label::new(label).size(LabelSize::Small)),
            )
            .tooltip(Tooltip::text("Switch Branch"))
            .on_click(|_, window, cx| {
                window.dispatch_action(Box::new(zed_actions::git::Branch), cx)
            })
            .into_any_element()
    }
}

impl StatusItemView for BranchStatusItem {
    fn set_active_pane_item(
        &mut self,
        _: Option<&dyn ItemHandle>,
        _window: &mut Window,
        _: &mut Context<Self>,
    ) {
    }

    fn hide_setting(&self, _: &App) -> Option<HideStatusItem> {
        None
    }
}
