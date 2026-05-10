use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteDecision {
    Direct,
    Broadcast,
}

pub fn route_decision(has_eligible_node: bool) -> RouteDecision {
    if has_eligible_node {
        RouteDecision::Direct
    } else {
        RouteDecision::Broadcast
    }
}

pub async fn run_with_timeout<F, T>(dur: Duration, fut: F) -> Result<T, String>
where
    F: Future<Output = T>,
{
    tokio::time::timeout(dur, fut)
        .await
        .map_err(|_| "distributed inference timeout".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn direct_inference_timeout() {
        let r = run_with_timeout(Duration::from_millis(10), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            42u32
        })
        .await;

        assert!(r.is_err());
    }

    #[test]
    fn fallback_to_broadcast_when_no_node() {
        assert_eq!(route_decision(false), RouteDecision::Broadcast);
        assert_eq!(route_decision(true), RouteDecision::Direct);
    }
}
