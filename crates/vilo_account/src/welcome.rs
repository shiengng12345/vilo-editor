use gpui::{
    App, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable, Subscription, WeakEntity,
    Window, prelude::*,
};
use ui::{
    Avatar, Button, ButtonLike, ButtonStyle, ContextMenu, Divider, PopoverMenu, Tooltip, prelude::*,
};
use workspace::{ModalView, Workspace};

use crate::{AccountEvent, AccountStore, MEEGLE_HOSTS, SignInStatus};

/// The sign-in screen shown on first launch and from the title bar. Skipping it
/// is always allowed: Vilo works the same without an account.
pub struct WelcomeModal {
    store: Entity<AccountStore>,
    host: usize,
    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

impl WelcomeModal {
    pub fn new(store: Entity<AccountStore>, _: &mut Window, cx: &mut Context<Self>) -> Self {
        let subscriptions = vec![
            cx.observe(&store, |_, _, cx| cx.notify()),
            cx.subscribe(&store, |_, _, event, cx| {
                if let AccountEvent::SignedIn = event {
                    cx.emit(DismissEvent);
                }
            }),
        ];
        Self {
            store,
            host: 0,
            focus_handle: cx.focus_handle(),
            _subscriptions: subscriptions,
        }
    }

    fn skip(&mut self, cx: &mut Context<Self>) {
        self.store.update(cx, |store, cx| {
            store.cancel_sign_in(cx);
            store.mark_welcome_seen(cx);
        });
        cx.emit(DismissEvent);
    }
}

impl EventEmitter<DismissEvent> for WelcomeModal {}

impl Focusable for WelcomeModal {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl ModalView for WelcomeModal {
    fn on_before_dismiss(
        &mut self,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) -> workspace::DismissDecision {
        // Escape or a click outside counts as "skip for now".
        self.store.update(cx, |store, cx| {
            store.cancel_sign_in(cx);
            store.mark_welcome_seen(cx);
        });
        workspace::DismissDecision::Dismiss(true)
    }

    fn fade_out_background(&self) -> bool {
        true
    }
}

impl Render for WelcomeModal {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let status = self.store.read(cx).status().clone();
        let in_progress = matches!(status, SignInStatus::InProgress { .. });
        let host = MEEGLE_HOSTS[self.host.min(MEEGLE_HOSTS.len() - 1)];

        let host_picker = h_flex()
            .gap_1()
            .children(MEEGLE_HOSTS.iter().enumerate().map(|(index, candidate)| {
                Button::new(("meegle-host", index), *candidate)
                    .label_size(LabelSize::Small)
                    .toggle_state(index == self.host)
                    .style(ButtonStyle::Subtle)
                    .disabled(in_progress)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.host = index;
                        cx.notify();
                    }))
            }));

        let primary = if in_progress {
            Button::new("cancel-sign-in", "Waiting for the browser… Cancel")
                .full_width()
                .style(ButtonStyle::Outlined)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.store.update(cx, |store, cx| store.cancel_sign_in(cx));
                }))
        } else {
            Button::new("sign-in-meegle", "Continue with Meegle")
                .full_width()
                .style(ButtonStyle::Filled)
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.store
                        .update(cx, |store, cx| store.sign_in_with_meegle(host, cx));
                }))
        };

        let coming_soon = |id: &'static str, label: &'static str| {
            Button::new(id, label)
                .full_width()
                .style(ButtonStyle::Outlined)
                .disabled(true)
                .tooltip(Tooltip::text("Coming soon"))
        };

        v_flex()
            .key_context("ViloWelcome")
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &menu::Cancel, _, cx| this.skip(cx)))
            .elevation_3(cx)
            .w(px(420.))
            .p_6()
            .gap_4()
            .child(
                v_flex()
                    .gap_1()
                    .child(Headline::new("Welcome to Vilo").size(HeadlineSize::Large))
                    .child(
                        Label::new("Sign in to link Vilo to your account. You can also skip this and sign in later from the title bar.")
                            .color(Color::Muted),
                    ),
            )
            .child(
                v_flex()
                    .gap_2()
                    .child(Label::new("Meegle site").size(LabelSize::Small).color(Color::Muted))
                    .child(host_picker)
                    .child(primary)
                    .child(coming_soon("sign-in-github", "Continue with GitHub"))
                    .child(coming_soon("sign-in-google", "Continue with Google")),
            )
            .when_some(
                match &status {
                    SignInStatus::Failed(message) => Some(message.clone()),
                    _ => None,
                },
                |this, message| this.child(Label::new(message).size(LabelSize::Small).color(Color::Error)),
            )
            .child(Divider::horizontal())
            .child(
                h_flex().justify_end().child(
                    Button::new("skip-sign-in", "Skip for now")
                        .style(ButtonStyle::Subtle)
                        .on_click(cx.listener(|this, _, _, cx| this.skip(cx))),
                ),
            )
    }
}

/// Title-bar entry: the signed-in person with a sign-out menu, or a "Sign In" button.
#[derive(IntoElement)]
pub struct AccountButton {
    workspace: WeakEntity<Workspace>,
}

impl AccountButton {
    pub fn new(workspace: WeakEntity<Workspace>) -> Self {
        Self { workspace }
    }
}

impl RenderOnce for AccountButton {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let Some(store) = AccountStore::global(cx) else {
            return gpui::Empty.into_any_element();
        };
        let Some(account) = store.read(cx).account().cloned() else {
            let workspace = self.workspace;
            return Button::new("vilo-sign-in", "Sign In")
                .label_size(LabelSize::Small)
                .on_click(move |_, window, cx| {
                    let store = store.clone();
                    workspace
                        .update(cx, |workspace, cx| {
                            workspace.toggle_modal(window, cx, |window, cx| {
                                WelcomeModal::new(store, window, cx)
                            })
                        })
                        .ok();
                })
                .into_any_element();
        };

        let trigger = ButtonLike::new("vilo-account")
            .tooltip(Tooltip::text(account.display_name.clone()))
            .child(match account.avatar_url.clone() {
                Some(avatar) => Avatar::new(avatar).size(rems(1.)).into_any_element(),
                None => Label::new(account.display_name.clone())
                    .size(LabelSize::Small)
                    .into_any_element(),
            });
        let host = url::Url::parse(&account.issuer)
            .ok()
            .and_then(|issuer| issuer.host_str().map(str::to_string))
            .unwrap_or_default();

        PopoverMenu::new("vilo-account-menu")
            .trigger(trigger)
            .anchor(gpui::Anchor::TopRight)
            .menu(move |window, cx| {
                let store = store.clone();
                let mut title = account.display_name.clone();
                if let Some(email) = &account.email {
                    title = format!("{title} · {email}");
                }
                let host = host.clone();
                Some(ContextMenu::build(window, cx, move |menu, _, _| {
                    menu.header(title)
                        .label(format!("Signed in with Meegle ({host})"))
                        .separator()
                        .entry("Sign Out", None, move |_, cx| {
                            store.update(cx, |store, cx| store.sign_out(cx));
                        })
                }))
            })
            .into_any_element()
    }
}
