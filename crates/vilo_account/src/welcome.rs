use gpui::{
    App, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable, Subscription, WeakEntity,
    Window, prelude::*,
};
use ui::{
    Avatar, Button, ButtonLike, ButtonSize, ButtonStyle, ContextMenu, Divider, PopoverMenu,
    TintColor, Tooltip, prelude::*,
};
use workspace::{ModalView, Workspace};

use crate::{AccountEvent, AccountStore, MEEGLE_HOSTS, SignInStatus, github::VERIFICATION_URL as GITHUB_VERIFICATION_URL};

/// The sign-in screen shown on first launch and from the title bar. Skipping it
/// is always allowed: Vilo works the same without an account.
pub struct WelcomeModal {
    store: Entity<AccountStore>,
    /// Shown when a provider that is not available yet is chosen.
    notice: Option<SharedString>,
    focus_handle: FocusHandle,
    _subscriptions: Vec<Subscription>,
}

#[derive(Clone, Copy)]
enum Provider {
    GitHub,
    Google,
    Meegle,
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
            notice: None,
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

    fn choose(&mut self, provider: Provider, cx: &mut Context<Self>) {
        self.notice = match provider {
            // shortcut: only the default Meegle site; other sites need a picker or setting.
            Provider::Meegle => {
                self.store.update(cx, |store, cx| {
                    store.sign_in_with_meegle(MEEGLE_HOSTS[0], cx)
                });
                None
            }
            Provider::GitHub => {
                self.store
                    .update(cx, |store, cx| store.sign_in_with_github(cx));
                None
            }
            Provider::Google => Some("Signing in with Google is coming soon.".into()),
        };
        cx.notify();
    }

    fn provider_button(
        &self,
        provider: Provider,
        in_progress: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let (id, icon, label) = match provider {
            Provider::GitHub => ("sign-in-github", IconName::Github, "Continue with GitHub"),
            Provider::Google => ("sign-in-google", IconName::AiGoogle, "Continue with Google"),
            Provider::Meegle => ("sign-in-meegle", IconName::Blocks, "Continue with Meegle"),
        };
        let style = match provider {
            Provider::GitHub => ButtonStyle::Tinted(TintColor::Accent),
            Provider::Google | Provider::Meegle => ButtonStyle::Outlined,
        };
        ButtonLike::new(id)
            .full_width()
            .size(ButtonSize::Large)
            .style(style)
            .disabled(in_progress)
            .child(
                h_flex()
                    .w_full()
                    .h(px(52.))
                    .justify_center()
                    .gap_2()
                    .child(Icon::new(icon).size(IconSize::Medium))
                    .child(Label::new(label).size(LabelSize::Large)),
            )
            .on_click(cx.listener(move |this, _, _, cx| this.choose(provider, cx)))
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
        // Escape or a click outside counts as "continue without signing in".
        self.store.update(cx, |store, cx| {
            store.cancel_sign_in(cx);
            store.mark_welcome_seen(cx);
        });
        workspace::DismissDecision::Dismiss(true)
    }

    // Drawn as its own full-window overlay so the card can be sized to the window.
    fn render_bare(&self) -> bool {
        true
    }
}

/// Share of the window the welcome card covers.
const CARD_FRACTION: f32 = 0.7;

impl Render for WelcomeModal {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let colors = cx.theme().colors();
        let viewport = window.viewport_size();
        let mut overlay = colors.elevated_surface_background;
        overlay.fade_out(0.2);
        let status = self.store.read(cx).status().clone();
        let in_progress = matches!(
            status,
            SignInStatus::InProgress { .. } | SignInStatus::GithubCode { .. }
        );

        let logo = div()
            .size(px(72.))
            .flex_none()
            .rounded_xl()
            .bg(colors.text_accent.opacity(0.35))
            .flex()
            .items_center()
            .justify_center()
            .child(Headline::new("V").size(HeadlineSize::Large));

        let github_code = match &status {
            SignInStatus::GithubCode { user_code } => Some(user_code.clone()),
            _ => None,
        };
        let note: Option<(SharedString, Color)> = match &status {
            SignInStatus::InProgress { .. } => {
                Some(("Finish signing in in your browser…".into(), Color::Muted))
            }
            SignInStatus::GithubCode { .. } => Some((
                "Enter this code on the GitHub page that just opened:".into(),
                Color::Muted,
            )),
            SignInStatus::Failed(message) => Some((message.clone(), Color::Error)),
            SignInStatus::Idle => self.notice.clone().map(|notice| (notice, Color::Muted)),
        };

        let content = v_flex()
            .w_full()
            .max_w(px(560.))
            .gap_6()
            .child(
                h_flex().gap_5().child(logo).child(
                    v_flex()
                        .gap_1()
                        .child(Headline::new("Welcome to Vilo").size(HeadlineSize::XLarge))
                        .child(
                            Label::new("Your workspace. Your tools. Your flow.")
                                .size(LabelSize::Large)
                                .color(Color::Muted),
                        ),
                ),
            )
            .child(Divider::horizontal())
            .child(
                v_flex()
                    .gap_3()
                    .child(self.provider_button(Provider::GitHub, in_progress, cx))
                    .child(self.provider_button(Provider::Google, in_progress, cx))
                    .child(self.provider_button(Provider::Meegle, in_progress, cx)),
            )
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        Icon::new(IconName::Lock)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        Label::new("Sign in to connect your work tools, like Meegle.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    ),
            )
            .when_some(note, |this, (message, color)| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(Label::new(message).size(LabelSize::Small).color(color))
                        .when_some(github_code.clone(), |this, user_code| {
                            this.child(
                                Label::new(user_code.clone())
                                    .size(LabelSize::Large)
                                    .weight(gpui::FontWeight::BOLD)
                                    .buffer_font(cx),
                            )
                            .child(
                                Button::new("copy-github-code", "Copy")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Subtle)
                                    .on_click(move |_, _, cx| {
                                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(
                                            user_code.to_string(),
                                        ))
                                    }),
                            )
                            .child(
                                Button::new("open-github-device", "Open GitHub")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Subtle)
                                    .on_click(|_, _, cx| cx.open_url(GITHUB_VERIFICATION_URL)),
                            )
                        })
                        .when(in_progress, |this| {
                            this.child(
                                Button::new("cancel-sign-in", "Cancel")
                                    .label_size(LabelSize::Small)
                                    .style(ButtonStyle::Subtle)
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.store.update(cx, |store, cx| store.cancel_sign_in(cx));
                                    })),
                            )
                        }),
                )
            })
            .child(Divider::horizontal())
            .child(
                h_flex()
                    .justify_between()
                    .child(
                        Label::new("Sign in anytime from the title bar.")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        Button::new("skip-sign-in", "Continue without signing in")
                            .style(ButtonStyle::Subtle)
                            .end_icon(Icon::new(IconName::ArrowRight).size(IconSize::Small))
                            .on_click(cx.listener(|this, _, _, cx| this.skip(cx))),
                    ),
            );

        div()
            .absolute()
            .inset_0()
            .size_full()
            .occlude()
            .bg(overlay)
            .flex()
            .items_center()
            .justify_center()
            // A click outside the card means "continue without signing in".
            .on_mouse_down(
                gpui::MouseButton::Left,
                cx.listener(|this, _, _, cx| this.skip(cx)),
            )
            .child(
                v_flex()
                    .key_context("ViloWelcome")
                    .track_focus(&self.focus_handle)
                    .on_action(cx.listener(|this, _: &menu::Cancel, _, cx| this.skip(cx)))
                    .on_mouse_down(gpui::MouseButton::Left, |_, _, cx| cx.stop_propagation())
                    .elevation_3(cx)
                    .w(viewport.width * CARD_FRACTION)
                    .h(viewport.height * CARD_FRACTION)
                    .p_8()
                    .items_center()
                    .justify_center()
                    .overflow_hidden()
                    .child(content),
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
        let workspace = self.workspace;
        let open_welcome = {
            let store = store.clone();
            move |window: &mut Window, cx: &mut App| {
                let store = store.clone();
                workspace
                    .update(cx, |workspace, cx| {
                        workspace.toggle_modal(window, cx, |window, cx| {
                            WelcomeModal::new(store, window, cx)
                        })
                    })
                    .ok();
            }
        };
        let account = store.read(cx).account().cloned();
        let github = store.read(cx).github().cloned();
        if account.is_none() && github.is_none() {
            return Button::new("vilo-sign-in", "Sign In")
                .label_size(LabelSize::Small)
                .on_click(move |_, window, cx| open_welcome(window, cx))
                .into_any_element();
        }

        let (display_name, avatar_url) = match (&account, &github) {
            (Some(account), _) => (account.display_name.clone(), account.avatar_url.clone()),
            (None, Some(github)) => (
                github.name.clone().unwrap_or_else(|| github.login.clone()),
                github.avatar_url.clone(),
            ),
            (None, None) => return gpui::Empty.into_any_element(),
        };
        let trigger = ButtonLike::new("vilo-account")
            .tooltip(Tooltip::text(display_name.clone()))
            .child(match avatar_url {
                Some(avatar) => Avatar::new(avatar).size(rems(1.)).into_any_element(),
                None => Label::new(display_name)
                    .size(LabelSize::Small)
                    .into_any_element(),
            });

        PopoverMenu::new("vilo-account-menu")
            .trigger(trigger)
            .anchor(gpui::Anchor::TopRight)
            .menu(move |window, cx| {
                let store = store.clone();
                let account = account.clone();
                let github = github.clone();
                let open_welcome = open_welcome.clone();
                Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                    if let Some(account) = &account {
                        let mut title = account.display_name.clone();
                        if let Some(email) = &account.email {
                            title = format!("{title} · {email}");
                        }
                        let host = url::Url::parse(&account.issuer)
                            .ok()
                            .and_then(|issuer| issuer.host_str().map(str::to_string))
                            .unwrap_or_default();
                        let store = store.clone();
                        menu = menu
                            .header(title)
                            .label(format!("Signed in with Meegle ({host})"))
                            .entry("Sign Out of Meegle", None, move |_, cx| {
                                store.update(cx, |store, cx| store.sign_out(cx));
                            })
                            .separator();
                    }
                    match &github {
                        Some(github) => {
                            let store = store.clone();
                            menu = menu
                                .header(format!("GitHub · @{}", github.login))
                                .entry("Disconnect GitHub", None, move |_, cx| {
                                    store.update(cx, |store, cx| store.disconnect_github(cx));
                                });
                        }
                        None => {
                            let open_welcome = open_welcome.clone();
                            menu = menu.entry("Connect GitHub…", None, move |window, cx| {
                                open_welcome(window, cx)
                            });
                        }
                    }
                    if account.is_none() {
                        let open_welcome = open_welcome.clone();
                        menu = menu.entry("Connect Meegle…", None, move |window, cx| {
                            open_welcome(window, cx)
                        });
                    }
                    menu
                }))
            })
            .into_any_element()
    }
}
