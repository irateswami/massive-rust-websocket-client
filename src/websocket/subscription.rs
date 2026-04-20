use std::collections::{HashMap, HashSet};

use crate::websocket::config::Topic;

/// Stores topic subscriptions for resubscribing after disconnect.
#[derive(Debug, Default)]
pub(crate) struct Subscriptions(HashMap<Topic, HashSet<String>>);

impl Subscriptions {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Inserts tickers for a topic. If "*" is in the list, replaces all existing tickers.
    pub fn add(&mut self, topic: Topic, tickers: &[String]) {
        if !self.0.contains_key(&topic) || tickers.iter().any(|t| t == "*") {
            self.0.insert(topic, HashSet::new());
        }
        let set = self.0.get_mut(&topic).unwrap();
        for t in tickers {
            set.insert(t.clone());
        }
    }

    /// Removes tickers from a topic. Removes the topic entirely if empty.
    pub fn delete(&mut self, topic: Topic, tickers: &[String]) {
        if let Some(set) = self.0.get_mut(&topic) {
            for t in tickers {
                set.remove(t);
            }
            if set.is_empty() {
                self.0.remove(&topic);
            }
        }
    }

    /// Returns subscription messages for all cached subscriptions (for reconnection replay).
    pub fn get_messages(&self) -> Vec<String> {
        self.0
            .iter()
            .filter_map(|(topic, tickers)| {
                let tickers: Vec<String> = tickers.iter().cloned().collect();
                build_subscribe_message("subscribe", *topic, &tickers).ok()
            })
            .collect()
    }

    /// Returns the set of tickers for a topic.
    pub fn get_tickers(&self, topic: &Topic) -> Vec<String> {
        self.0
            .get(topic)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }
}

/// Builds a JSON subscription/unsubscription message.
pub(crate) fn build_subscribe_message(
    action: &str,
    topic: Topic,
    tickers: &[String],
) -> Result<String, serde_json::Error> {
    let tickers = if tickers.is_empty() {
        vec!["*".to_string()]
    } else {
        tickers.to_vec()
    };

    let params: Vec<String> = tickers
        .iter()
        .map(|t| format!("{}.{}", topic.prefix(), t))
        .collect();

    serde_json::to_string(&serde_json::json!({
        "action": action,
        "params": params.join(","),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- build_subscribe_message (port of TestGetSub) ---

    #[test]
    fn test_build_sub_multiple_tickers() {
        let msg = build_subscribe_message(
            "subscribe",
            Topic::StocksMinAggs,
            &["AAPL".into(), "GME".into(), "HOOD".into()],
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["action"], "subscribe");
        assert_eq!(parsed["params"], "AM.AAPL,AM.GME,AM.HOOD");
    }

    #[test]
    fn test_build_unsub_no_tickers_defaults_wildcard() {
        let msg = build_subscribe_message("unsubscribe", Topic::StocksSecAggs, &[]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(parsed["action"], "unsubscribe");
        assert_eq!(parsed["params"], "A.*");
    }

    // --- Subscriptions::get_messages (port of TestGet) ---

    #[test]
    fn test_get_after_subscribe() {
        let mut subs = Subscriptions::new();
        subs.add(Topic::StocksMinAggs, &["AAPL".into()]);
        let msgs = subs.get_messages();
        assert_eq!(msgs.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(&msgs[0]).unwrap();
        assert_eq!(parsed["action"], "subscribe");
        assert!(parsed["params"].as_str().unwrap().contains("AM.AAPL"));
    }

    #[test]
    fn test_get_after_unsubscribe_all() {
        let mut subs = Subscriptions::new();
        subs.add(Topic::StocksMinAggs, &["AAPL".into()]);

        // Unsubscribe removes the cached ticker.
        subs.delete(Topic::StocksMinAggs, &["AAPL".into()]);
        let msgs = subs.get_messages();
        assert!(msgs.is_empty());
    }

    #[test]
    fn test_get_second_topic() {
        let mut subs = Subscriptions::new();
        subs.add(Topic::StocksTrades, &["SNAP".into()]);
        let msgs = subs.get_messages();
        assert_eq!(msgs.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(&msgs[0]).unwrap();
        assert_eq!(parsed["action"], "subscribe");
        assert!(parsed["params"].as_str().unwrap().contains("T.SNAP"));
    }

    // --- Full lifecycle (port of TestSubscriptions) ---

    #[test]
    fn test_full_subscription_lifecycle() {
        let mut subs = Subscriptions::new();

        // Subscribe to AAPL and TSLA.
        subs.add(Topic::StocksMinAggs, &["AAPL".into(), "TSLA".into()]);
        assert!(
            subs.get_tickers(&Topic::StocksMinAggs)
                .contains(&"AAPL".into())
        );
        assert!(
            subs.get_tickers(&Topic::StocksMinAggs)
                .contains(&"TSLA".into())
        );

        // Unsubscribe AAPL (and nonexistent NFLX — should be harmless).
        subs.delete(Topic::StocksMinAggs, &["AAPL".into(), "NFLX".into()]);
        assert!(
            !subs
                .get_tickers(&Topic::StocksMinAggs)
                .contains(&"AAPL".into())
        );
        assert!(
            subs.get_tickers(&Topic::StocksMinAggs)
                .contains(&"TSLA".into())
        );

        // Subscribe with no tickers → wildcard replaces all.
        subs.add(Topic::StocksMinAggs, &["*".into()]);
        let tickers = subs.get_tickers(&Topic::StocksMinAggs);
        assert!(tickers.contains(&"*".into()));
        assert!(!tickers.contains(&"TSLA".into()));

        // Unsubscribe wildcard.
        subs.delete(Topic::StocksMinAggs, &["*".into()]);
        assert!(subs.get_tickers(&Topic::StocksMinAggs).is_empty());

        // Trades topic — unsubscribe nonexistent shouldn't panic.
        subs.delete(Topic::StocksTrades, &["RDFN".into()]);
        assert!(subs.get_tickers(&Topic::StocksTrades).is_empty());

        // Subscribe to FB.
        subs.add(Topic::StocksTrades, &["FB".into()]);
        assert!(
            subs.get_tickers(&Topic::StocksTrades)
                .contains(&"FB".into())
        );

        // Unsubscribe all tickers from trades.
        let all = subs.get_tickers(&Topic::StocksTrades);
        subs.delete(Topic::StocksTrades, &all);
        assert!(subs.get_tickers(&Topic::StocksTrades).is_empty());
    }

    #[test]
    fn test_add_specific_tickers() {
        let mut subs = Subscriptions::new();
        subs.add(Topic::StocksTrades, &["AAPL".into(), "TSLA".into()]);
        let tickers = subs.get_tickers(&Topic::StocksTrades);
        assert!(tickers.contains(&"AAPL".to_string()));
        assert!(tickers.contains(&"TSLA".to_string()));
        assert_eq!(tickers.len(), 2);
    }

    #[test]
    fn test_add_wildcard_replaces_existing() {
        let mut subs = Subscriptions::new();
        subs.add(Topic::StocksTrades, &["AAPL".into(), "TSLA".into()]);
        assert_eq!(subs.get_tickers(&Topic::StocksTrades).len(), 2);

        subs.add(Topic::StocksTrades, &["*".into()]);
        let tickers = subs.get_tickers(&Topic::StocksTrades);
        assert_eq!(tickers, vec!["*".to_string()]);
    }

    #[test]
    fn test_delete_all_removes_topic_entry() {
        let mut subs = Subscriptions::new();
        subs.add(Topic::StocksTrades, &["AAPL".into()]);
        subs.delete(Topic::StocksTrades, &["AAPL".into()]);
        assert!(subs.get_tickers(&Topic::StocksTrades).is_empty());
        // Topic should be fully removed (not just empty set).
        assert_eq!(subs.get_messages().len(), 0);
    }
}
