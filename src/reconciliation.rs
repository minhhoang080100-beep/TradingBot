//! Read-only, bounded account inventory and comparison with local execution.
//!
//! A completed scan means every configured partition was read, not that all
//! exchange products, bots, fills or account risks were covered. REST pages are
//! not an atomic snapshot. Empty lists never prove an uncertain order was not
//! accepted, and this module cannot construct an admission/flatness certificate.
//! Keep execution halted, cancel scans on private-stream continuity loss, and
//! reconcile cumulative order evidence before making lifecycle decisions.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Method;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::{
    ApiCredentials, Exchange, ExecutionCoordinator, ExecutionError, OrderKind, OrderRef,
    RestExecutor, SignedRequest,
};

#[cfg(feature = "binance")]
pub mod binance;
#[cfg(feature = "bybit")]
pub mod bybit;
#[cfg(feature = "okx")]
pub mod okx;

/// Explicit API coverage, never a claim to cover the entire exchange ecosystem.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryScope {
    /// Standard USD-M futures endpoints, excluding Portfolio Margin and SAPI bots.
    BinanceUsdM,
    /// Linear USDT/USDC orders and positions; excludes other UTA products.
    BybitLinearUsdtUsdc,
    /// Standard trading orders, pending algos and positions; excludes separate bots.
    OkxTrading,
}

impl InventoryScope {
    /// Venue owning these endpoints.
    #[must_use]
    pub const fn exchange(self) -> Exchange {
        match self {
            Self::BinanceUsdM => Exchange::Binance,
            Self::BybitLinearUsdtUsdc => Exchange::Bybit,
            Self::OkxTrading => Exchange::Okx,
        }
    }
}

/// Logical inventory domain; ordinary and algo venue IDs have separate namespaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum InventoryKind {
    /// Account-wide position-mode evidence, when the API provides it.
    Mode,
    /// Ordinary open orders.
    Orders,
    /// Pending conditional/algo orders.
    AlgoOrders,
    /// Position snapshots, including zero rows when returned.
    Positions,
}

/// One explicitly configured endpoint/filter scope, exhaustively paginated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InventoryPartition {
    /// Stable adapter-owned identifier; no credentials or user input.
    pub key: &'static str,
    /// Domain used for comparison and duplicate detection.
    pub kind: InventoryKind,
}

/// Minimum exact evidence needed to detect foreign orders and exposure.
///
/// This intentionally lacks fill/terminal semantics and cannot release risk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InventoryRecord {
    /// A currently listed order, potentially placed outside this bot.
    Order {
        /// Exact venue symbol, not a canonical alias.
        symbol: String,
        /// Parent venue ID in this inventory domain.
        venue_order_id: String,
        /// External orders can have no client-supplied ID.
        client_order_id: Option<String>,
    },
    /// Position quantity without converting contract units or netting hedge legs.
    Position {
        /// Exact venue symbol.
        symbol: String,
        /// Stable ID or venue-specific symbol/side identity.
        position_id: String,
        /// Exact signed quantity if supplied; any nonzero amount is exposure.
        quantity: Decimal,
    },
    /// True only when a dedicated account-mode endpoint proves net/one-way mode.
    Mode {
        /// False may also mean a hedge leg was observed in an inventory row.
        one_way: bool,
    },
}

/// Parsed page. A missing/malformed cursor field must not be treated as exhaustion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryPage {
    /// Validated records, including foreign orders and unsupported product exposure.
    pub records: Vec<InventoryRecord>,
    /// Opaque continuation; `None` only when the endpoint proves exhaustion.
    pub next_cursor: Option<String>,
}

/// Venue-specific read-only requests and strict inventory parsing.
pub trait InventoryAdapter: Send + Sync {
    /// Venue for request routing.
    fn exchange(&self) -> Exchange;
    /// Exact API scope, including its documented exclusions.
    fn scope(&self) -> InventoryScope;
    /// Exhaustive list for this declared scope; stable for the scan duration.
    fn partitions(&self) -> Vec<InventoryPartition>;
    /// Builds one freshly signed GET request without changing account settings.
    ///
    /// # Errors
    /// Rejects invalid partition/cursor, credentials or signing time.
    fn prepare(
        &self,
        partition: &InventoryPartition,
        cursor: Option<&str>,
        credentials: &ApiCredentials,
        at: DateTime<Utc>,
    ) -> Result<SignedRequest, ExecutionError>;
    /// Parses required fields and exact decimal strings, never a success default.
    ///
    /// # Errors
    /// Rejects HTTP/venue errors, malformed fields or ambiguous pagination.
    fn parse(
        &self,
        partition: &InventoryPartition,
        status: u16,
        body: &[u8],
    ) -> Result<InventoryPage, ExecutionError>;
}

/// Transport response. Debug excludes sensitive account response bytes.
pub struct InventoryResponse {
    /// HTTP status.
    pub status: u16,
    /// Exact body; never log it or treat it as a fill without a dedicated parser.
    pub body: Bytes,
}

impl fmt::Debug for InventoryResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InventoryResponse")
            .field("status", &self.status)
            .field("body", &"[REDACTED]")
            .finish()
    }
}

/// Read-only transport seam for deterministic offline tests and pinned REST clients.
#[async_trait]
pub trait InventoryTransport: Send + Sync {
    /// Prevents using credentials/requests with a differently pinned venue client.
    fn exchange(&self) -> Exchange;
    /// Sends a signed GET once. Implementations must not log authentication data.
    ///
    /// # Errors
    /// Returns transport/HTTP failures without substituting empty inventory.
    async fn fetch(&self, request: &SignedRequest) -> Result<InventoryResponse, ExecutionError>;
}

#[async_trait]
impl InventoryTransport for RestExecutor {
    fn exchange(&self) -> Exchange {
        self.exchange()
    }

    async fn fetch(&self, request: &SignedRequest) -> Result<InventoryResponse, ExecutionError> {
        if request.method() != Method::GET || !request.body().is_empty() {
            return Err(ExecutionError::InvalidRequest(
                "inventory must be read-only",
            ));
        }
        let response = self.send_signed(request).await?;
        Ok(InventoryResponse {
            status: response.status(),
            body: response.body().clone(),
        })
    }
}

/// Hard caps for one scan. No unbounded cursor loops or automatic retries.
#[derive(Clone, Copy, Debug)]
pub struct InventoryLimits {
    max_pages_per_partition: usize,
    max_records: usize,
    timeout: Duration,
}

impl InventoryLimits {
    /// Validates bounded pages (1..=1000), rows (1..=100000), and timeout (0..=120s).
    ///
    /// # Errors
    /// Rejects zero or excessive limits.
    pub fn new(pages: usize, records: usize, timeout: Duration) -> Result<Self, InventoryError> {
        if !(1..=1000).contains(&pages)
            || !(1..=100_000).contains(&records)
            || timeout.is_zero()
            || timeout > Duration::from_secs(120)
        {
            return Err(InventoryError::InvalidLimits);
        }
        Ok(Self {
            max_pages_per_partition: pages,
            max_records: records,
            timeout,
        })
    }
}

impl Default for InventoryLimits {
    fn default() -> Self {
        Self {
            max_pages_per_partition: 100,
            max_records: 20_000,
            timeout: Duration::from_secs(30),
        }
    }
}

/// A record together with the endpoint/filter domain in which it was observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedInventoryRecord {
    /// Originating scope.
    pub partition: InventoryPartition,
    /// Account evidence; contains sensitive identifiers and quantities.
    pub record: InventoryRecord,
}

/// Completed, non-atomic observation. No conversion to `FlatAccountSnapshot` exists.
pub struct InventoryReport {
    exchange: Exchange,
    scope: InventoryScope,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    pages: usize,
    records: Vec<ObservedInventoryRecord>,
}

impl fmt::Debug for InventoryReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InventoryReport")
            .field("exchange", &self.exchange)
            .field("scope", &self.scope)
            .field("pages", &self.pages)
            .field("records", &self.records.len())
            .finish_non_exhaustive()
    }
}

impl InventoryReport {
    /// Exact declared scope, not an all-products/account-completeness assertion.
    #[must_use]
    pub const fn scope(&self) -> InventoryScope {
        self.scope
    }
    /// First request time. Freshness is measured from here, not the last page.
    #[must_use]
    pub const fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
    /// Local receipt time after the last page; not the exchange's snapshot time.
    #[must_use]
    pub const fn finished_at(&self) -> DateTime<Utc> {
        self.finished_at
    }
    /// Number of successful HTTP pages across every configured partition.
    #[must_use]
    pub const fn pages(&self) -> usize {
        self.pages
    }
    /// Sensitive account evidence. Inspect only; never infer terminal order state.
    #[must_use]
    pub fn records(&self) -> &[ObservedInventoryRecord] {
        &self.records
    }
    /// Position mode evidence; `None` means unknown, even when no positions exist.
    #[must_use]
    pub fn one_way_mode(&self) -> Option<bool> {
        self.records.iter().find_map(|row| match row.record {
            InventoryRecord::Mode { one_way } => Some(one_way),
            _ => None,
        })
    }

    /// Compares identities only and requires cumulative queries for ALL locally
    /// unresolved orders, including those missing from the open-order lists.
    /// Does not mutate the coordinator, reservations or entry-admission state.
    ///
    /// # Errors
    /// Rejects wrong venue, future timestamps, backwards clocks or stale scans.
    pub fn review(
        &self,
        execution: &ExecutionCoordinator,
        now: DateTime<Utc>,
        max_age: Duration,
    ) -> Result<InventoryReview, InventoryError> {
        self.review_with_queries(execution, execution.reconciliation_orders(), now, max_age)
    }

    /// Includes unresolved durable outbox operations as well as local brackets.
    /// Observed historical bot orders are queried again, never treated as new
    /// entries or cancelled automatically. This method commits no journal record.
    /// Credential/account ownership must still be verified by the supervisor.
    ///
    /// # Errors
    /// Rejects stale observations or a differently scoped venue.
    pub fn review_account(
        &self,
        account: &crate::DurableAccount,
        now: DateTime<Utc>,
        max_age: Duration,
    ) -> Result<InventoryReview, InventoryError> {
        let queries = account
            .recovery_queries()
            .into_iter()
            .filter_map(|command| match command {
                crate::OrderCommand::Query(reference) => Some(reference),
                _ => None,
            })
            .collect();
        self.review_with_queries(account.execution(), queries, now, max_age)
    }

    fn review_with_queries(
        &self,
        execution: &ExecutionCoordinator,
        required_queries: Vec<OrderRef>,
        now: DateTime<Utc>,
        max_age: Duration,
    ) -> Result<InventoryReview, InventoryError> {
        if execution.exchange() != self.exchange {
            return Err(InventoryError::WrongVenue);
        }
        let age = now
            .signed_duration_since(self.started_at)
            .to_std()
            .map_err(|_| InventoryError::Stale)?;
        if max_age.is_zero()
            || self.finished_at > now
            || self.finished_at < self.started_at
            || age > max_age
        {
            return Err(InventoryError::Stale);
        }
        let known = execution.checkpoint_order_references();
        let by_client: HashMap<_, _> = known
            .iter()
            .map(|order| (order.client_order_id.as_str(), order))
            .collect();
        let by_venue: HashMap<_, _> = known
            .iter()
            .filter_map(|order| {
                order.venue_order_id.as_deref().map(|id| {
                    (
                        (
                            order_domain(order.kind),
                            order.instrument.venue_symbol.as_str(),
                            id,
                        ),
                        order,
                    )
                })
            })
            .collect();
        let mut queries: HashMap<_, _> = required_queries
            .into_iter()
            .map(|order| (order.client_order_id.clone(), order))
            .collect();
        let mut unmatched = Vec::new();
        let mut observed = HashSet::new();
        let mut nonzero_positions = 0;
        let mut open_orders = 0;
        for row in &self.records {
            match &row.record {
                InventoryRecord::Order {
                    symbol,
                    venue_order_id,
                    client_order_id,
                } => {
                    open_orders += 1;
                    let matching = client_order_id
                        .as_deref()
                        .and_then(|id| by_client.get(id))
                        .or_else(|| {
                            by_venue.get(&(
                                row.partition.kind,
                                symbol.as_str(),
                                venue_order_id.as_str(),
                            ))
                        })
                        .filter(|order| {
                            identity_matches(
                                order,
                                row.partition.kind,
                                symbol,
                                venue_order_id,
                                client_order_id.as_deref(),
                            )
                        });
                    if let Some(order) = matching {
                        observed.insert(order.client_order_id.clone());
                        queries.insert(order.client_order_id.clone(), (*order).clone());
                    } else {
                        unmatched.push(row.clone());
                    }
                }
                InventoryRecord::Position { quantity, .. } if !quantity.is_zero() => {
                    nonzero_positions += 1;
                }
                _ => {}
            }
        }
        let mut local: Vec<_> = queries.into_values().collect();
        local.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
        let missing = local
            .iter()
            .filter(|order| !observed.contains(&order.client_order_id))
            .cloned()
            .collect();
        Ok(InventoryReview {
            query_orders: local,
            absent_from_open_lists: missing,
            unmatched_orders: unmatched,
            open_orders,
            nonzero_positions,
            one_way_mode: self.one_way_mode(),
        })
    }
}

fn identity_matches(
    order: &OrderRef,
    kind: InventoryKind,
    symbol: &str,
    venue_id: &str,
    client_id: Option<&str>,
) -> bool {
    order.instrument.venue_symbol.as_str() == symbol
        && order_domain(order.kind) == kind
        && client_id.is_none_or(|id| id == order.client_order_id)
        && order
            .venue_order_id
            .as_deref()
            .is_none_or(|id| id == venue_id)
}

/// Inspection results, not authorization to trade or evidence of cancellation.
#[derive(Clone)]
pub struct InventoryReview {
    query_orders: Vec<OrderRef>,
    absent_from_open_lists: Vec<OrderRef>,
    unmatched_orders: Vec<ObservedInventoryRecord>,
    open_orders: usize,
    nonzero_positions: usize,
    one_way_mode: Option<bool>,
}

impl InventoryReview {
    /// Query every unresolved local order for cumulative/terminal evidence.
    #[must_use]
    pub fn query_orders(&self) -> &[OrderRef] {
        &self.query_orders
    }
    /// Missing means query/history inspection is required, NEVER cancel/fill proof.
    #[must_use]
    pub fn absent_from_open_lists(&self) -> &[OrderRef] {
        &self.absent_from_open_lists
    }
    /// Foreign or identity-conflicting orders; never automatically cancel them.
    #[must_use]
    pub fn unmatched_orders(&self) -> &[ObservedInventoryRecord] {
        &self.unmatched_orders
    }
    /// Observed pending orders, regardless of ownership.
    #[must_use]
    pub const fn open_orders(&self) -> usize {
        self.open_orders
    }
    /// Count of nonzero rows without netting or offsetting separate hedge legs.
    #[must_use]
    pub const fn nonzero_positions(&self) -> usize {
        self.nonzero_positions
    }
    /// Mode evidence; unknown does not default to one-way.
    #[must_use]
    pub const fn one_way_mode(&self) -> Option<bool> {
        self.one_way_mode
    }
}

fn order_domain(kind: OrderKind) -> InventoryKind {
    match kind {
        OrderKind::StopMarket | OrderKind::TakeProfitMarket => InventoryKind::AlgoOrders,
        OrderKind::Limit | OrderKind::Market => InventoryKind::Orders,
    }
}

/// Performs every partition sequentially with fresh signatures and no retries.
///
/// This low-level seam trusts the caller's adapter, transport, credential/account
/// binding and stream-continuity cancellation. Prefer `RestExecutor::inventory`
/// to use the same credential source as the pinned execution transport. Report
/// creation never resumes trading. Cancelled/failed scans return no partial report.
///
/// # Errors
/// Fails on any page error, repeated cursor/row, inconsistent mode, oversized
/// response, cap, timeout, cancellation or venue mismatch.
pub async fn scan_inventory(
    adapter: &dyn InventoryAdapter,
    credentials: &ApiCredentials,
    transport: &dyn InventoryTransport,
    limits: InventoryLimits,
    cancel: &CancellationToken,
) -> Result<InventoryReport, InventoryError> {
    if adapter.exchange() != transport.exchange()
        || adapter.scope().exchange() != adapter.exchange()
    {
        return Err(InventoryError::WrongVenue);
    }
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(InventoryError::Cancelled),
        result = tokio::time::timeout(limits.timeout, collect(adapter, credentials, transport, limits)) => {
            result.map_err(|_| InventoryError::Timeout)?
        }
    }
}

async fn collect(
    adapter: &dyn InventoryAdapter,
    credentials: &ApiCredentials,
    transport: &dyn InventoryTransport,
    limits: InventoryLimits,
) -> Result<InventoryReport, InventoryError> {
    let partitions = adapter.partitions();
    validate_partitions(&partitions)?;
    let mut report = InventoryReport {
        exchange: adapter.exchange(),
        scope: adapter.scope(),
        started_at: Utc::now(),
        finished_at: Utc::now(),
        pages: 0,
        records: Vec::new(),
    };
    let mut seen_records = HashSet::new();
    let mut mode = None;
    for partition in partitions {
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        let mut pages = 0;
        loop {
            if pages >= limits.max_pages_per_partition {
                return Err(InventoryError::Capacity);
            }
            let request =
                adapter.prepare(&partition, cursor.as_deref(), credentials, Utc::now())?;
            if request.method() != Method::GET || !request.body().is_empty() {
                return Err(InventoryError::NotReadOnly);
            }
            let response = transport.fetch(&request).await?;
            if response.body.len() > 1_048_576 {
                return Err(InventoryError::Capacity);
            }
            if !(200..300).contains(&response.status) {
                return Err(ExecutionError::HttpStatus(response.status).into());
            }
            let page = adapter.parse(&partition, response.status, &response.body)?;
            if page.records.len() > limits.max_records.saturating_sub(report.records.len()) {
                return Err(InventoryError::Capacity);
            }
            for record in page.records {
                validate_record(&partition, &record, &mut seen_records, &mut mode)?;
                report
                    .records
                    .push(ObservedInventoryRecord { partition, record });
            }
            pages += 1;
            report.pages += 1;
            match page.next_cursor {
                None => break,
                Some(next) => {
                    if !valid_text(&next, 2048) || !cursors.insert(next.clone()) {
                        return Err(InventoryError::InvalidPagination);
                    }
                    cursor = Some(next);
                }
            }
        }
    }
    report.finished_at = Utc::now();
    if report.finished_at < report.started_at {
        return Err(InventoryError::Stale);
    }
    Ok(report)
}

fn validate_partitions(partitions: &[InventoryPartition]) -> Result<(), InventoryError> {
    let mut keys = HashSet::new();
    if partitions.is_empty()
        || partitions.len() > 32
        || partitions
            .iter()
            .any(|p| !valid_text(p.key, 64) || !keys.insert(p.key))
        || [
            InventoryKind::Orders,
            InventoryKind::AlgoOrders,
            InventoryKind::Positions,
        ]
        .iter()
        .any(|kind| !partitions.iter().any(|p| p.kind == *kind))
    {
        return Err(InventoryError::InvalidPartition);
    }
    Ok(())
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && value.bytes().all(|b| b.is_ascii_graphic())
}

fn validate_record(
    partition: &InventoryPartition,
    record: &InventoryRecord,
    seen: &mut HashSet<(InventoryKind, String, String)>,
    mode: &mut Option<bool>,
) -> Result<(), InventoryError> {
    let identity = match record {
        InventoryRecord::Order {
            symbol,
            venue_order_id,
            client_order_id,
        } => {
            if !matches!(
                partition.kind,
                InventoryKind::Orders | InventoryKind::AlgoOrders
            ) || !valid_text(symbol, 128)
                || !valid_text(venue_order_id, 256)
                || client_order_id
                    .as_ref()
                    .is_some_and(|id| !valid_text(id, 256))
            {
                return Err(InventoryError::InvalidRecord);
            }
            if let Some(client_id) = client_order_id {
                if !seen.insert((
                    partition.kind,
                    symbol.clone(),
                    format!("client:{client_id}"),
                )) {
                    return Err(InventoryError::DuplicateRecord);
                }
            }
            // Some venues allocate order IDs per symbol, not account-wide.
            (
                partition.kind,
                symbol.clone(),
                format!("venue:{venue_order_id}"),
            )
        }
        InventoryRecord::Position {
            symbol,
            position_id,
            ..
        } => {
            if partition.kind != InventoryKind::Positions
                || !valid_text(symbol, 128)
                || !valid_text(position_id, 256)
            {
                return Err(InventoryError::InvalidRecord);
            }
            (partition.kind, symbol.clone(), position_id.clone())
        }
        InventoryRecord::Mode { one_way } => {
            if (*one_way && partition.kind != InventoryKind::Mode)
                || mode.is_some_and(|prior| prior != *one_way)
            {
                return Err(InventoryError::InvalidRecord);
            }
            *mode = Some(*one_way);
            return Ok(());
        }
    };
    if !seen.insert(identity) {
        return Err(InventoryError::DuplicateRecord);
    }
    Ok(())
}

/// Sanitized scan failure. No body, token, cursor or account values are retained.
#[derive(Debug, thiserror::Error)]
pub enum InventoryError {
    /// Adapter, signature, protocol or HTTP failure.
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    /// Locally invalid resource limits.
    #[error("invalid inventory limits")]
    InvalidLimits,
    /// Adapter and transport scopes disagree.
    #[error("inventory venue mismatch")]
    WrongVenue,
    /// Missing, duplicate or invalid partition plan.
    #[error("invalid inventory partitions")]
    InvalidPartition,
    /// A purported read-only adapter attempted a mutating request.
    #[error("inventory request is not read-only")]
    NotReadOnly,
    /// Missing record identity, wrong domain or conflicting mode evidence.
    #[error("invalid inventory record")]
    InvalidRecord,
    /// Same row repeated across pages; concurrent mutation may have disturbed pagination.
    #[error("duplicate inventory record")]
    DuplicateRecord,
    /// Empty/oversized/invalid/repeated opaque cursor.
    #[error("invalid inventory pagination")]
    InvalidPagination,
    /// Row, page or response bound exceeded.
    #[error("inventory capacity exceeded")]
    Capacity,
    /// Overall scan deadline, not per-page retry advice.
    #[error("inventory scan timed out")]
    Timeout,
    /// Shutdown/stream continuity token invalidated the scan.
    #[error("inventory scan cancelled")]
    Cancelled,
    /// Stale/future/inconsistent local receipt times.
    #[error("inventory scan is stale")]
    Stale,
}

#[cfg(test)]
mod tests;
