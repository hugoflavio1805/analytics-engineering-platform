# Enrich the three raw CSVs with insight columns.
# Reads from data/raw_original/ and writes to data/raw/.
# Idempotent: re-run anytime; original files in raw_original are never modified.

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$src  = Join-Path $root 'data\raw_original'
$dst  = Join-Path $root 'data\raw'
New-Item -ItemType Directory -Force -Path $dst | Out-Null

$today = Get-Date '2026-05-08'

# ---------- helpers ----------
function Norm-Id($id) {
    if ([string]::IsNullOrWhiteSpace($id)) { return $null }
    return ($id.Trim() -replace '_','-').ToUpper() -replace '^SUB-','sub-'
}
function Norm-CustId($id) {
    if ([string]::IsNullOrWhiteSpace($id)) { return $null }
    return ($id.Trim() -replace '_','-').ToUpper()
}
function Parse-Date($s) {
    if ([string]::IsNullOrWhiteSpace($s)) { return $null }
    try { return [datetime]::ParseExact($s.Trim(), 'yyyy-MM-dd', $null) } catch { return $null }
}
function Bool-Or-Null($s) {
    if ([string]::IsNullOrWhiteSpace($s)) { return '' }
    switch ($s.Trim().ToLower()) {
        'true'  { 'true' }
        'false' { 'false' }
        default { '' }
    }
}

# ---------- load ----------
$customers     = Import-Csv (Join-Path $src 'customers.csv')
$subscriptions = Import-Csv (Join-Path $src 'subscriptions.csv')
$feedbacks     = Import-Csv (Join-Path $src 'feedbacks.csv')

Write-Host "Loaded: $($customers.Count) customers, $($subscriptions.Count) subscriptions, $($feedbacks.Count) feedbacks"

# ---------- index subscriptions by normalized customer_id ----------
$subsByCust = @{}
foreach ($s in $subscriptions) {
    $cid = Norm-CustId $s.customer_id
    if ([string]::IsNullOrWhiteSpace($cid)) { continue }
    if (-not $subsByCust.ContainsKey($cid)) { $subsByCust[$cid] = @() }
    $subsByCust[$cid] += $s
}

# ---------- index feedbacks by normalized customer_id ----------
$fbByCust = @{}
foreach ($f in $feedbacks) {
    $cid = Norm-CustId $f.customer_id
    if ([string]::IsNullOrWhiteSpace($cid)) { continue }
    if (-not $fbByCust.ContainsKey($cid)) { $fbByCust[$cid] = @() }
    $fbByCust[$cid] += $f
}

# ============================================================
# 1. CUSTOMERS — enriched
# ============================================================
$enrichedCustomers = foreach ($c in $customers) {
    $cid = Norm-CustId $c.customer_id
    $created = Parse-Date $c.created_at
    $tenureDays = if ($created) { [int]($today - $created).TotalDays } else { $null }

    $custSubs = @(if ($cid -and $subsByCust.ContainsKey($cid)) { $subsByCust[$cid] } else { @() })
    $activeSubs    = @($custSubs | Where-Object { $_.status -eq 'active' })
    $cancelledSubs = @($custSubs | Where-Object { $_.status -eq 'cancelled' })
    $isActivePaying  = $activeSubs.Count -gt 0
    $totalMrr        = ($activeSubs | Measure-Object mrr -Sum).Sum
    if (-not $totalMrr) { $totalMrr = 0 }

    $churnDate = $null
    if ($cancelledSubs.Count -gt 0) {
        $churnDate = ($cancelledSubs | ForEach-Object { Parse-Date $_.end_date } | Where-Object { $_ } | Sort-Object -Descending | Select-Object -First 1)
    }
    $daysToChurn = if ($churnDate -and $created) { [int]($churnDate - $created).TotalDays } else { $null }

    $custFbs = @(if ($cid -and $fbByCust.ContainsKey($cid)) { $fbByCust[$cid] } else { @() })
    $fbCount     = $custFbs.Count
    $fbBugs      = @($custFbs | Where-Object { $_.type -eq 'bug' }).Count
    $fbComplain  = @($custFbs | Where-Object { $_.type -eq 'complaint' }).Count
    $fbPraise    = @($custFbs | Where-Object { $_.type -eq 'praise' }).Count
    $fbFeature   = @($custFbs | Where-Object { $_.type -eq 'feature_request' }).Count
    $negFb       = $fbBugs + $fbComplain
    $churnRiskScore = if ($fbCount -eq 0) { 0 } else { [math]::Round($negFb / [double]$fbCount, 2) }

    $mrrBand = if ($totalMrr -ge 500) { 'high' }
               elseif ($totalMrr -ge 200) { 'medium' }
               elseif ($totalMrr -gt 0)   { 'low' }
               else { 'none' }

    [pscustomobject][ordered]@{
        customer_id        = $cid
        name               = $c.name
        email              = $c.email
        plan               = $c.plan
        segment            = $c.segment
        status             = $c.status
        created_at         = $c.created_at
        country            = $c.country
        tenure_days        = $tenureDays
        is_active_paying   = $isActivePaying.ToString().ToLower()
        total_mrr          = $totalMrr
        mrr_band           = $mrrBand
        subscriptions_count= $custSubs.Count
        churn_date         = if ($churnDate) { $churnDate.ToString('yyyy-MM-dd') } else { '' }
        days_to_churn      = if ($daysToChurn) { $daysToChurn } else { '' }
        feedback_count     = $fbCount
        negative_feedback_count = $negFb
        praise_count       = $fbPraise
        feature_request_count = $fbFeature
        churn_risk_score   = $churnRiskScore
    }
}
$enrichedCustomers | Export-Csv -Path (Join-Path $dst 'customers.csv') -NoTypeInformation -Encoding utf8
Write-Host "Wrote enriched customers.csv ($($enrichedCustomers.Count) rows, +12 insight columns)"

# ============================================================
# 2. SUBSCRIPTIONS — enriched
# ============================================================
$enrichedSubs = foreach ($s in $subscriptions) {
    $sid   = ($s.subscription_id.Trim() -replace '_','-').ToLower()
    $cid   = Norm-CustId $s.customer_id
    $start = Parse-Date $s.start_date
    $end   = Parse-Date $s.end_date
    $isActive = $s.status -eq 'active'
    $endRef = if ($end) { $end } else { $today }
    $lifetimeDays = if ($start) { [int]($endRef - $start).TotalDays } else { $null }
    $months = if ($lifetimeDays) { [math]::Max(1, [math]::Round($lifetimeDays / 30.0, 0)) } else { 1 }
    $mrr = [int]$s.mrr
    $ltvSoFar = $mrr * $months
    $isAnnual = $s.billing_cycle -eq 'annual'
    $arr = if ($isAnnual) { $mrr * 12 } else { $mrr * 12 }

    $churnedWithin90 = ''
    if ($end -and $start) {
        $churnedWithin90 = ((($end - $start).TotalDays -le 90)).ToString().ToLower()
    }

    [pscustomobject][ordered]@{
        subscription_id  = $sid
        customer_id      = $cid
        plan             = $s.plan
        status           = $s.status
        mrr              = $mrr
        arr              = $arr
        start_date       = $s.start_date
        end_date         = $s.end_date
        billing_cycle    = $s.billing_cycle
        is_active        = $isActive.ToString().ToLower()
        lifetime_days    = $lifetimeDays
        ltv_so_far       = $ltvSoFar
        churned_within_90d = $churnedWithin90
    }
}
$enrichedSubs | Export-Csv -Path (Join-Path $dst 'subscriptions.csv') -NoTypeInformation -Encoding utf8
Write-Host "Wrote enriched subscriptions.csv ($($enrichedSubs.Count) rows, +5 insight columns)"

# ============================================================
# 3. FEEDBACKS — enriched
# ============================================================
$negTypes = @('bug','complaint')
$posTypes = @('praise')

# build a quick map of churn date per customer (from subscriptions) for "days_before_churn"
$churnDateByCust = @{}
foreach ($cid in $subsByCust.Keys) {
    $cancelled = $subsByCust[$cid] | Where-Object { $_.status -eq 'cancelled' }
    if ($cancelled.Count -gt 0) {
        $latest = ($cancelled | ForEach-Object { Parse-Date $_.end_date } | Where-Object { $_ } | Sort-Object -Descending | Select-Object -First 1)
        if ($latest) { $churnDateByCust[$cid] = $latest }
    }
}

# detect duplicates: same customer + same message + same date
$dupKey = @{}
foreach ($f in $feedbacks) {
    $k = "$(Norm-CustId $f.customer_id)|$($f.message)|$($f.created_at)"
    if (-not $dupKey.ContainsKey($k)) { $dupKey[$k] = 0 }
    $dupKey[$k]++
}

$enrichedFb = foreach ($f in $feedbacks) {
    $cid     = Norm-CustId $f.customer_id
    $created = Parse-Date $f.created_at
    $isNeg   = $negTypes -contains $f.type
    $isPos   = $posTypes -contains $f.type

    $sentScore = switch ($f.sentiment) {
        'positive' { 1 }
        'negative' { -1 }
        'neutral'  { 0 }
        default    { '' }
    }

    $msgLen  = if ($f.message) { $f.message.Length } else { 0 }
    $hasNps  = -not [string]::IsNullOrWhiteSpace($f.nps_score)
    $npsBand = ''
    if ($hasNps) {
        $n = [int]$f.nps_score
        $npsBand = if ($n -ge 9) { 'promoter' } elseif ($n -ge 7) { 'passive' } else { 'detractor' }
    }

    $daysBeforeChurn = ''
    $isPreChurn      = 'false'
    if ($cid -and $churnDateByCust.ContainsKey($cid) -and $created) {
        $diff = [int]($churnDateByCust[$cid] - $created).TotalDays
        if ($diff -ge 0) {
            $daysBeforeChurn = $diff
            if ($diff -le 90) { $isPreChurn = 'true' }
        }
    }

    $k = "$cid|$($f.message)|$($f.created_at)"
    $isDup = ($dupKey[$k] -gt 1).ToString().ToLower()

    [pscustomobject][ordered]@{
        feedback_id        = $f.feedback_id
        customer_id        = $cid
        type               = $f.type
        channel            = $f.channel
        sentiment          = $f.sentiment
        sentiment_score    = $sentScore
        message            = $f.message
        message_length     = $msgLen
        created_at         = $f.created_at
        nps_score          = $f.nps_score
        nps_band           = $npsBand
        resolved           = Bool-Or-Null $f.resolved
        is_negative        = $isNeg.ToString().ToLower()
        is_positive        = $isPos.ToString().ToLower()
        days_before_churn  = $daysBeforeChurn
        is_pre_churn_90d   = $isPreChurn
        is_duplicate_candidate = $isDup
    }
}
$enrichedFb | Export-Csv -Path (Join-Path $dst 'feedbacks.csv') -NoTypeInformation -Encoding utf8
Write-Host "Wrote enriched feedbacks.csv ($($enrichedFb.Count) rows, +9 insight columns)"

Write-Host "`nDone. Originals untouched in data\raw_original\, enriched files in data\raw\."
