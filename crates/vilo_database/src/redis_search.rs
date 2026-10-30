//! The Redis Search tab, as RedisInsight's: choose a RediSearch index, see its definition, run a
//! query and page through the matching keys with their fields. Read only; a key opens in Browse.

use editor::Editor;
use gpui::{
    Entity, ListHorizontalSizingBehavior, UniformListScrollHandle, Window, prelude::*, uniform_list,
};
use ui::{Button, ButtonStyle, ContextMenu, PopoverMenu, Tooltip, prelude::*};
use vilo_database_core::{
    redis::Bytes,
    redis_search::{
        Content, DEFAULT_PAGE, DEFAULT_TIMEOUT_MS, DOCUMENT_BYTES, IndexInfo, MAX_INDEXES,
        MAX_VALUE_BYTES, PAGE_BYTES, SearchPage, SearchRequest,
    },
};

use crate::{DatabaseView, boxed, editor as single_line, redis_view::escape_bytes, run};

/// Characters of a result's fields shown on its row.
const ROW_CHARACTERS: usize = 400;

pub(crate) struct SearchState {
    query: Option<Entity<Editor>>,
    indexes: Option<Result<(Vec<Bytes>, bool), SharedString>>,
    index: Option<Bytes>,
    info: Option<Result<IndexInfo, SharedString>>,
    page_size: u64,
    timeout_ms: u64,
    /// The request the shown page answers, for paging.
    shown: Option<SearchRequest>,
    page: Option<Result<SearchPage, SharedString>>,
    /// The page's rows as shown, built once when it arrives: key, key text, missing, fields text.
    rows: Vec<(Bytes, String, bool, String)>,
    running: bool,
    /// Bumped by every index choice and every search, so an answer to an older one is dropped.
    info_attempt: u64,
    search_attempt: u64,
    scroll: UniformListScrollHandle,
}

impl Default for SearchState {
    fn default() -> Self {
        Self {
            query: None,
            indexes: None,
            index: None,
            info: None,
            page_size: DEFAULT_PAGE,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            shown: None,
            page: None,
            rows: Vec::new(),
            running: false,
            info_attempt: 0,
            search_attempt: 0,
            scroll: UniformListScrollHandle::new(),
        }
    }
}

fn fields_text(content: &Content) -> (bool, String) {
    match content {
        Content::Missing(reason) => (true, reason.clone()),
        Content::Fields(fields) => {
            let mut text = String::new();
            let mut characters = 0;
            for field in fields {
                if characters >= ROW_CHARACTERS {
                    break;
                }
                let mut part = String::new();
                if !text.is_empty() {
                    part.push_str("  ·  ");
                }
                part.push_str(&escape_bytes(&field.name));
                part.push_str(": ");
                // No character takes more than four bytes, so this prefix fills the row.
                let shown = field.value.len().min(ROW_CHARACTERS * 4);
                part.push_str(&escape_bytes(&field.value[..shown]));
                characters += part.chars().count();
                text.push_str(&part);
            }
            let mut text = if characters > ROW_CHARACTERS {
                let mut text = text.chars().take(ROW_CHARACTERS).collect::<String>();
                text.push_str(" …");
                text
            } else {
                text
            };
            // Said after the row is shortened, so the note itself is never cut off.
            let cut = fields
                .iter()
                .filter(|field| field.size > field.value.len())
                .count();
            if cut > 0 {
                text.push_str(&format!(
                    "  [values over {} KB are cut ({cut}); open the key to see them in full]",
                    MAX_VALUE_BYTES / 1024
                ));
            }
            (false, text)
        }
    }
}

fn info_text(info: &IndexInfo) -> String {
    let mut parts = vec![if info.key_type.is_empty() {
        "Unknown key type".to_string()
    } else {
        info.key_type.clone()
    }];
    parts.push(if info.prefixes.is_empty() {
        "every key".into()
    } else {
        format!("keys starting with {}", info.prefixes.join(", "))
    });
    if let Some(filter) = &info.filter {
        parts.push(format!("filter {filter}"));
    }
    if let Some(documents) = info.documents {
        parts.push(format!("{documents} documents"));
    }
    if let Some(percent) = info.percent_indexed
        && percent < 1.0
    {
        parts.push(format!("still indexing ({:.0} %)", percent * 100.0));
    }
    if let Some(failures) = info.failures
        && failures > 0
    {
        parts.push(format!("{failures} keys failed to index"));
    }
    if !info.attributes.is_empty() {
        parts.push(format!(
            "fields: {}",
            info.attributes
                .iter()
                .map(|attribute| {
                    if attribute.identifier == attribute.name || attribute.identifier.is_empty() {
                        format!("{} ({})", attribute.name, attribute.kind)
                    } else {
                        format!(
                            "{} = {} ({})",
                            attribute.name, attribute.identifier, attribute.kind
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    parts.join(" · ")
}

impl DatabaseView {
    fn search_state(&mut self) -> Option<&mut SearchState> {
        self.redis_state.as_mut().map(|state| &mut state.search)
    }

    pub(crate) fn open_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(search) = self.search_state() else {
            return;
        };
        if search.query.is_none() {
            let query = single_line("Query, e.g. * or @title:apple", "*", false, window, cx);
            if let Some(search) = self.search_state() {
                search.query = Some(query);
            }
        }
        let load = self
            .search_state()
            .is_some_and(|search| !matches!(search.indexes, Some(Ok(_))));
        if load {
            self.load_search_indexes(cx);
        }
        cx.notify();
    }

    fn load_search_indexes(&mut self, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let generation = self.generation;
        let Some(search) = self.search_state() else {
            return;
        };
        search.indexes = None;
        let task = run(cx, async move { session.search_indexes().await });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(search) = this.search_state() else {
                    return;
                };
                let first = result
                    .as_ref()
                    .ok()
                    .and_then(|(names, _)| names.first().cloned());
                let listed = result.is_ok();
                search.indexes = Some(result.map_err(|error| format!("{error:#}").into()));
                let keep = search.index.as_ref().is_some_and(|index| {
                    matches!(&search.indexes, Some(Ok((names, _))) if names.contains(index))
                });
                if listed && !keep {
                    // The chosen index is gone: nothing about it, or still on its way, is shown.
                    search.index = None;
                    search.info = None;
                    search.page = None;
                    search.rows.clear();
                    search.shown = None;
                    search.running = false;
                    search.info_attempt += 1;
                    search.search_attempt += 1;
                    if let Some(first) = first {
                        this.choose_search_index(first, cx);
                    }
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    fn choose_search_index(&mut self, index: Bytes, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let generation = self.generation;
        let Some(search) = self.search_state() else {
            return;
        };
        search.index = Some(index.clone());
        search.info = None;
        search.page = None;
        search.rows.clear();
        search.shown = None;
        search.running = false;
        search.info_attempt += 1;
        search.search_attempt += 1;
        let attempt = search.info_attempt;
        let task = run(cx, async move { session.search_index_info(&index).await });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                if let Some(search) = this.search_state()
                    && search.info_attempt == attempt
                {
                    search.info = Some(result.map_err(|error| format!("{error:#}").into()));
                    cx.notify();
                }
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    /// Runs the typed query from its first page, or `offset` of the shown one when paging.
    fn run_search(&mut self, offset: Option<u64>, cx: &mut Context<Self>) {
        let Some(session) = self.redis() else {
            return;
        };
        let generation = self.generation;
        let Some(search) = self.search_state() else {
            return;
        };
        let request = match (offset, &search.shown) {
            (Some(offset), Some(shown)) => SearchRequest {
                offset,
                count: search.page_size,
                timeout_ms: search.timeout_ms,
                ..shown.clone()
            },
            _ => {
                let Some(index) = search.index.clone() else {
                    return;
                };
                SearchRequest {
                    index,
                    query: search
                        .query
                        .as_ref()
                        .map(|query| query.read(cx).text(cx))
                        .unwrap_or_default(),
                    offset: 0,
                    count: search.page_size,
                    timeout_ms: search.timeout_ms,
                }
            }
        };
        search.running = true;
        search.search_attempt += 1;
        let attempt = search.search_attempt;
        let task = run(cx, {
            let request = request.clone();
            async move { session.search(&request).await }
        });
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                if this.generation != generation {
                    return;
                }
                let Some(search) = this
                    .search_state()
                    .filter(|search| search.search_attempt == attempt)
                else {
                    return;
                };
                search.running = false;
                search.rows = match &result {
                    Ok(page) => page
                        .hits
                        .iter()
                        .map(|hit| {
                            let (missing, text) = fields_text(&hit.content);
                            (hit.key.clone(), escape_bytes(&hit.key), missing, text)
                        })
                        .collect(),
                    Err(_) => Vec::new(),
                };
                search.page = Some(result.map_err(|error| format!("{error:#}").into()));
                search.shown = Some(request);
                cx.notify();
            })
            .ok();
        })
        .detach();
        cx.notify();
    }

    fn open_search_hit(&mut self, key: Bytes, cx: &mut Context<Self>) {
        if let Some(state) = self.redis_state.as_mut() {
            state.tab = crate::redis_view::RedisTab::Browse;
        }
        self.reopen_redis_key(key, cx);
        cx.notify();
    }

    pub(crate) fn render_search(&self, search: &SearchState, cx: &mut Context<Self>) -> AnyElement {
        let colors = cx.theme().colors().clone();
        let view = cx.entity().downgrade();
        let index_label = search
            .index
            .as_ref()
            .map_or("Choose an index".to_string(), |index| escape_bytes(index));
        let index_menu = PopoverMenu::new("database-search-index")
            .trigger(
                Button::new("database-search-index-button", index_label)
                    .label_size(LabelSize::Small)
                    .style(ButtonStyle::Outlined)
                    .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
            )
            .menu({
                let names = match &search.indexes {
                    Some(Ok((names, _))) => names.clone(),
                    _ => Vec::new(),
                };
                let view = view.clone();
                move |window, cx| {
                    let names = names.clone();
                    let view = view.clone();
                    Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                        if names.is_empty() {
                            menu = menu.label("No indexes");
                        }
                        for name in names {
                            let view = view.clone();
                            menu = menu.entry(escape_bytes(&name), None, move |_, cx| {
                                let name = name.clone();
                                view.update(cx, |this, cx| this.choose_search_index(name, cx))
                                    .ok();
                            });
                        }
                        menu
                    }))
                }
            });
        let choice_menu = |id: &'static str,
                           label: String,
                           choices: &'static [u64],
                           describe: fn(u64) -> String,
                           set: fn(&mut SearchState, u64)| {
            let view = view.clone();
            PopoverMenu::new(id)
                .trigger(
                    Button::new(SharedString::from(format!("{id}-button")), label)
                        .label_size(LabelSize::Small)
                        .style(ButtonStyle::Subtle)
                        .end_icon(Icon::new(IconName::ChevronDown).size(IconSize::XSmall)),
                )
                .menu(move |window, cx| {
                    let view = view.clone();
                    Some(ContextMenu::build(window, cx, move |mut menu, _, _| {
                        for &choice in choices {
                            let view = view.clone();
                            menu = menu.entry(describe(choice), None, move |_, cx| {
                                view.update(cx, |this, cx| {
                                    if let Some(search) = this.search_state() {
                                        set(search, choice);
                                    }
                                    cx.notify();
                                })
                                .ok();
                            });
                        }
                        menu
                    }))
                })
        };
        let page_menu = choice_menu(
            "database-search-page",
            format!("{} per page", search.page_size),
            &[10, 20, 50, 100],
            |count| format!("{count} per page"),
            |search, count| search.page_size = count,
        );
        let timeout_menu = choice_menu(
            "database-search-timeout",
            format!("Time limit {} ms", search.timeout_ms),
            &[500, 1000, 5000, 10_000],
            |timeout| format!("{timeout} ms"),
            |search, timeout| search.timeout_ms = timeout,
        );

        let indexes_note = match &search.indexes {
            None => Some(("Loading indexes…".to_string(), Color::Muted)),
            Some(Err(error)) => Some((error.to_string(), Color::Warning)),
            Some(Ok((names, _))) if names.is_empty() => Some((
                "This database has no search indexes. Create one with FT.CREATE.".to_string(),
                Color::Muted,
            )),
            Some(Ok((_, true))) => Some((
                format!("Only the first {MAX_INDEXES} indexes are listed."),
                Color::Warning,
            )),
            Some(Ok(_)) => None,
        };
        let info_note = match &search.info {
            None if search.index.is_some() => {
                Some(("Loading the index…".to_string(), Color::Muted))
            }
            None => None,
            Some(Ok(info)) => Some((info_text(info), Color::Muted)),
            Some(Err(error)) => Some((error.to_string(), Color::Warning)),
        };

        let rows = search.rows.clone();
        let summary = match &search.page {
            Some(Ok(page)) => {
                let offset = search.shown.as_ref().map_or(0, |shown| shown.offset);
                let summary = if page.hits.is_empty() {
                    format!("No results on this page · {} found", page.total)
                } else {
                    format!(
                        "{}–{} of {} for {}",
                        offset + 1,
                        offset + page.hits.len() as u64,
                        page.total,
                        search
                            .shown
                            .as_ref()
                            .map_or(String::new(), |shown| shown.query.clone())
                    )
                };
                Some((summary, Color::Default))
            }
            Some(Err(error)) => Some((error.to_string(), Color::Warning)),
            None => None,
        };
        let partial = match &search.page {
            Some(Ok(page)) => page.partial.clone(),
            _ => None,
        };
        let (previous, next) = match (&search.page, &search.shown) {
            (Some(Ok(page)), Some(shown)) => (
                (shown.offset > 0).then(|| shown.offset.saturating_sub(search.page_size)),
                (shown.offset + (page.hits.len() as u64) < page.total
                    && page.hits.len() as u64 == shown.count)
                    .then(|| shown.offset + shown.count),
            ),
            _ => (None, None),
        };
        let widest = rows
            .iter()
            .enumerate()
            .max_by_key(|(_, row)| row.1.len() + row.3.len())
            .map(|(index, _)| index);
        let list = uniform_list("database-search-results", rows.len(), {
            let view = view.clone();
            move |range, _, cx| {
                range
                    .filter_map(|index| rows.get(index).cloned().map(|row| (index, row)))
                    .map(|(index, (key, shown_key, missing, text))| {
                        let view = view.clone();
                        h_flex()
                            .id(("database-search-hit", index))
                            .px_2()
                            .gap_3()
                            .whitespace_nowrap()
                            .cursor_pointer()
                            .hover(|style| style.bg(cx.theme().colors().ghost_element_hover))
                            .tooltip(Tooltip::text("Open in Browse"))
                            .on_click(move |_, _, cx| {
                                let key = key.clone();
                                view.update(cx, |this, cx| this.open_search_hit(key, cx))
                                    .ok();
                            })
                            .child(
                                div().w(px(260.)).flex_none().child(
                                    Label::new(shown_key)
                                        .size(LabelSize::Small)
                                        .buffer_font(cx)
                                        .single_line()
                                        .truncate(),
                                ),
                            )
                            .child(
                                Label::new(text)
                                    .size(LabelSize::Small)
                                    .buffer_font(cx)
                                    .color(if missing {
                                        Color::Muted
                                    } else {
                                        Color::Default
                                    }),
                            )
                            .into_any_element()
                    })
                    .collect::<Vec<_>>()
            }
        })
        .with_horizontal_sizing_behavior(ListHorizontalSizingBehavior::Unconstrained)
        .with_width_from_item(widest)
        .track_scroll(&search.scroll)
        .flex_1();
        let note =
            |(text, color): (String, Color)| Label::new(text).size(LabelSize::Small).color(color);
        let can_run = search.index.is_some() && !search.running;
        v_flex()
            .size_full()
            .p_3()
            .gap_2()
            .child(
                h_flex()
                    .gap_2()
                    .child(index_menu)
                    .child(
                        IconButton::new("database-search-reload", IconName::ArrowCircle)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Reload the indexes"))
                            .on_click(cx.listener(|this, _, _, cx| this.load_search_indexes(cx))),
                    )
                    .child(
                        div().flex_1().child(
                            search
                                .query
                                .clone()
                                .map(|query| boxed(query, cx).into_any_element())
                                .unwrap_or_else(|| div().into_any_element()),
                        ),
                    )
                    .child(page_menu)
                    .child(timeout_menu)
                    .child(
                        Button::new(
                            "database-search-run",
                            if search.running { "Searching…" } else { "Search" },
                        )
                        .style(ButtonStyle::Filled)
                        .disabled(!can_run)
                        .on_click(cx.listener(|this, _, _, cx| this.run_search(None, cx))),
                    ),
            )
            .when_some(indexes_note, |this, text| this.child(note(text)))
            .when_some(info_note, |this, text| this.child(note(text)))
            .child(
                Label::new(format!(
                    "Read only (FT.SEARCH). The server stops a query at its time limit and may then return only part of the results; Vilo says so. Content is fetched for at most {} MB of documents per page, documents over {} MB and field values over {} KB are not shown in full; open the key in Browse to see it all. Indexes cover database 0 only.",
                    PAGE_BYTES / (1024 * 1024),
                    DOCUMENT_BYTES / (1024 * 1024),
                    MAX_VALUE_BYTES / 1024
                ))
                .size(LabelSize::Small)
                .color(Color::Muted),
            )
            .when_some(partial, |this, reason| {
                this.child(note((
                    format!("Results may be incomplete: {reason}"),
                    Color::Warning,
                )))
            })
            .when_some(summary, |this, summary| {
                this.child(
                    h_flex()
                        .gap_2()
                        .child(note(summary))
                        .child(div().flex_1())
                        .child(
                            Button::new("database-search-previous", "Previous")
                                .label_size(LabelSize::Small)
                                .style(ButtonStyle::Subtle)
                                .disabled(previous.is_none() || search.running)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if let Some(offset) = previous {
                                        this.run_search(Some(offset), cx);
                                    }
                                })),
                        )
                        .child(
                            Button::new("database-search-next", "Next")
                                .label_size(LabelSize::Small)
                                .style(ButtonStyle::Subtle)
                                .disabled(next.is_none() || search.running)
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    if let Some(offset) = next {
                                        this.run_search(Some(offset), cx);
                                    }
                                })),
                        ),
                )
            })
            .child(
                v_flex()
                    .flex_1()
                    .min_h_0()
                    .py_1()
                    .rounded_md()
                    .border_1()
                    .border_color(colors.border)
                    .bg(colors.editor_background)
                    .child(list),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vilo_database_core::redis_search::{Field, IndexAttribute};

    #[test]
    fn test_result_rows_are_short_and_say_what_was_cut() {
        let field = |name: &str, value: &[u8], size: usize| Field {
            name: name.as_bytes().to_vec(),
            value: value.to_vec(),
            size,
        };
        let (missing, text) = fields_text(&Content::Fields(vec![
            field("n", b"1", 1),
            field("title", b"red\napple", 9),
        ]));
        assert!(!missing);
        assert_eq!(text, "n: 1  ·  title: red\\x0aapple");

        let (_, text) = fields_text(&Content::Fields(vec![field("body", b"abc", 70_000)]));
        assert_eq!(
            text,
            "body: abc  [values over 64 KB are cut (1); open the key to see them in full]"
        );

        let long = vec![b'x'; 2000];
        let (_, text) = fields_text(&Content::Fields(vec![
            field("a", &long, 2000),
            field("b", b"never shown", 11),
        ]));
        assert_eq!(text.chars().count(), ROW_CHARACTERS + 2);
        assert!(text.ends_with(" …") && !text.contains("never"));

        let (_, text) = fields_text(&Content::Fields(vec![field("a", &long, 70_000)]));
        assert!(
            text.ends_with("…  [values over 64 KB are cut (1); open the key to see them in full]"),
            "the note survives shortening: {text}"
        );

        let (missing, text) = fields_text(&Content::Missing("gone".into()));
        assert!(missing);
        assert_eq!(text, "gone");
    }

    #[test]
    fn test_index_summary() {
        let info = IndexInfo {
            key_type: "JSON".into(),
            prefixes: vec!["user:".into()],
            filter: None,
            attributes: vec![
                IndexAttribute {
                    identifier: "$.name".into(),
                    name: "name".into(),
                    kind: "TEXT".into(),
                },
                IndexAttribute {
                    identifier: "age".into(),
                    name: "age".into(),
                    kind: "NUMERIC".into(),
                },
            ],
            documents: Some(3),
            percent_indexed: Some(0.5),
            failures: Some(1),
        };
        assert_eq!(
            info_text(&info),
            "JSON · keys starting with user: · 3 documents · still indexing (50 %) · 1 keys failed to index · fields: name = $.name (TEXT), age (NUMERIC)"
        );
    }
}
