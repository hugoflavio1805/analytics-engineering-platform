# Generate additional synthetic raw rows that share the schema and the
# realistic data-quality issues of the original dataset (mixed id formats,
# duplicates, missing NPS scores, blank "resolved" flags, etc.).
#
# It APPENDS to the existing data/raw/*.csv files without modifying their
# header. The originals in data/raw_original/ are never touched.
#
# Run:
#   powershell -ExecutionPolicy Bypass -File scripts\generate_synthetic_data.ps1 -ExtraCustomers 100

param(
    [int]$ExtraCustomers = 100,
    [int]$Seed           = 42
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dst  = Join-Path $root 'data\raw'

$rng = [System.Random]::new($Seed)

# ---------- pools (mirror the original distributions) ----------
$plans       = @('starter', 'starter', 'starter', 'growth', 'growth', 'enterprise')
$segments    = @{ 'starter' = 'SMB'; 'growth' = 'Mid-Market'; 'enterprise' = 'Enterprise' }
$mrrByPlan   = @{ 'starter' = 99;    'growth' = 349;          'enterprise' = 999 }
$statuses    = @('active','active','active','active','active','churned')
$countries   = @('BR','BR','BR','BR','BR','MX','US','AR','CL')
$channels    = @('nps','suporte','formulario','email','plataforma')
$fbTypes     = @('praise','praise','complaint','bug','feature_request','support')
$sentimentByType = @{
    'praise'          = 'positive'
    'complaint'       = 'negative'
    'bug'             = 'negative'
    'feature_request' = 'neutral'
    'support'         = 'neutral'
}
# Use unicode escapes to avoid any encoding ambiguity when this script is read.
# \u{NNNN} works on PS 6+; for PS 5.1 we build characters via [char]0xNNNN and string concat.
$_a_acute = [char]0x00E1   # á
$_i_acute = [char]0x00ED   # í
$_o_acute = [char]0x00F3   # ó
$_u_acute = [char]0x00FA   # ú
$_a_tilde = [char]0x00E3   # ã
$_c_ced   = [char]0x00E7   # ç

$messages = @{
    'praise' = @(
        "Suporte muito r${_a_acute}pido",
        "Plataforma excelente!",
        "Melhorou muito nos ${_u_acute}ltimos meses",
        "Equipe atenciosa",
        "Funcionalidade impec${_a_acute}vel"
    )
    'complaint' = @(
        "Tempo de resposta do suporte muito alto",
        "Faturamento incorreto",
        "Dif${_i_acute}cil de configurar",
        "Sem retorno do CSM"
    )
    'bug' = @(
        "Integra${_c_ced}${_a_tilde}o com API retorna timeout",
        "Erro 500 ao acessar dashboard",
        "Relat${_o_acute}rio gerado em branco",
        "Login intermitente"
    )
    'feature_request' = @(
        "Gostar${_i_acute}amos de exportar em PDF",
        "Seria ${_o_acute}timo ter alertas por email",
        "Falta integra${_c_ced}${_a_tilde}o com HubSpot",
        "SSO via Okta"
    )
    'support' = @(
        "Como fa${_c_ced}o para adicionar usu${_a_acute}rios?",
        "Como recupero a fatura?",
        "Posso mudar de plano?"
    )
}

# ---------- counters: continue from where the source CSVs left off ----------
$existingCustomers = Import-Csv (Join-Path $dst 'customers.csv')
$existingSubs      = Import-Csv (Join-Path $dst 'subscriptions.csv')
$existingFbs       = Import-Csv (Join-Path $dst 'feedbacks.csv')

[int]$nextCustomerNum = 1 + (($existingCustomers | ForEach-Object { [int]($_.customer_id -replace 'CUST[-_]', '') } | Measure-Object -Maximum).Maximum)
[int]$nextSubNum      = 1 + (($existingSubs      | ForEach-Object { [int]($_.subscription_id -replace 'sub[-_]|SUB[-_]', '') } | Measure-Object -Maximum).Maximum)
[int]$nextFbNum       = 1 + (($existingFbs       | ForEach-Object { [int]($_.feedback_id -replace 'FB-', '') } | Measure-Object -Maximum).Maximum)

Write-Host ("Continuing from CUST-{0:D4}, sub-{1:D4}, FB-{2:D5}" -f $nextCustomerNum, $nextSubNum, $nextFbNum)

function Pick($arr) { return $arr[$rng.Next(0, $arr.Count)] }
function RandomDate([datetime]$min, [datetime]$max) {
    $span = ($max - $min).TotalDays
    return $min.AddDays($rng.Next(0, [int]$span))
}
function MaybeMangleCustId($id) {
    # 8% chance of using underscore form (mirrors the original quality issue)
    if ($rng.NextDouble() -lt 0.08) { return ($id -replace '-','_') } else { return $id }
}
function MaybeMangleSubId($id) {
    # 30% upper-case + hyphen, 70% lower-case + underscore
    if ($rng.NextDouble() -lt 0.30) { return ($id.ToUpper() -replace '_','-') } else { return $id }
}

# ---------- generators ----------
$newCustomers     = New-Object System.Collections.Generic.List[object]
$newSubs          = New-Object System.Collections.Generic.List[object]
$newFbs           = New-Object System.Collections.Generic.List[object]

$startWindow = [datetime]'2022-01-01'
$endWindow   = [datetime]'2025-06-01'

for ($i = 0; $i -lt $ExtraCustomers; $i++) {
    $custNum = $nextCustomerNum + $i
    $custId  = "CUST-{0:D4}" -f $custNum
    $plan    = Pick $plans
    $segment = $segments[$plan]
    $status  = Pick $statuses
    $created = RandomDate $startWindow $endWindow.AddMonths(-3)
    $country = Pick $countries

    $newCustomers.Add([pscustomobject]@{
        customer_id = $custId
        name        = "Empresa $custNum"
        email       = "contato$custNum@empresa$custNum.com.br"
        plan        = $plan
        segment     = $segment
        status      = $status
        created_at  = $created.ToString('yyyy-MM-dd')
        country     = $country
    })

    # 1 subscription per customer (sometimes 2 for re-subscribers)
    $subCount = if ($rng.NextDouble() -lt 0.10) { 2 } else { 1 }
    for ($s = 0; $s -lt $subCount; $s++) {
        $subNum   = $nextSubNum
        $nextSubNum++
        $rawSubId = "sub_{0:D4}" -f $subNum
        $subId    = MaybeMangleSubId $rawSubId
        $subStart = $created.AddDays($rng.Next(0, 14))
        $subStatus = if ($status -eq 'churned' -or $rng.NextDouble() -lt 0.15) { 'cancelled' } else { 'active' }
        $endDate   = if ($subStatus -eq 'cancelled') {
            (RandomDate $subStart.AddMonths(1) $endWindow).ToString('yyyy-MM-dd')
        } else { '' }
        $billing  = if ($rng.NextDouble() -lt 0.20) { 'annual' } else { 'monthly' }

        $newSubs.Add([pscustomobject]@{
            subscription_id = $subId
            customer_id     = $custId
            plan            = $plan
            status          = $subStatus
            mrr             = $mrrByPlan[$plan]
            start_date      = $subStart.ToString('yyyy-MM-dd')
            end_date        = $endDate
            billing_cycle   = $billing
        })
    }

    # 0..4 feedbacks per customer, mostly biased by status
    $fbCount = $rng.Next(0, 5)
    if ($status -eq 'churned') { $fbCount = [math]::Max($fbCount, 1) }

    for ($f = 0; $f -lt $fbCount; $f++) {
        $fbNum = $nextFbNum
        $nextFbNum++
        $fbId  = "FB-{0:D5}" -f $fbNum
        $type  = Pick $fbTypes
        if ($status -eq 'churned' -and $rng.NextDouble() -lt 0.55) {
            $type = Pick @('bug','complaint','complaint','bug','feature_request')
        }
        $sent     = $sentimentByType[$type]
        $channel  = Pick $channels
        $msg      = Pick $messages[$type]
        $fbDate   = RandomDate $created $endWindow
        $nps      = if ($channel -eq 'nps') {
            if ($sent -eq 'positive') { $rng.Next(8, 11) }
            elseif ($sent -eq 'negative') { $rng.Next(0, 7) }
            else { $rng.Next(6, 9) }
        } else { '' }
        $resolvedRaw = $rng.NextDouble()
        $resolved = if ($resolvedRaw -lt 0.45) { 'True' } elseif ($resolvedRaw -lt 0.85) { 'False' } else { '' }

        $rawCustId = MaybeMangleCustId $custId

        $newFbs.Add([pscustomobject]@{
            feedback_id = $fbId
            customer_id = $rawCustId
            type        = $type
            channel     = $channel
            sentiment   = $sent
            message     = $msg
            created_at  = $fbDate.ToString('yyyy-MM-dd')
            nps_score   = $nps
            resolved    = $resolved
        })

        # 4% chance of injecting a near-duplicate (same customer, same message, same date)
        if ($rng.NextDouble() -lt 0.04) {
            $fbNumDup = $nextFbNum
            $nextFbNum++
            $newFbs.Add([pscustomobject]@{
                feedback_id = "FB-{0:D5}" -f $fbNumDup
                customer_id = $rawCustId
                type        = $type
                channel     = $channel
                sentiment   = $sent
                message     = $msg
                created_at  = $fbDate.ToString('yyyy-MM-dd')
                nps_score   = $nps
                resolved    = $resolved
            })
        }
    }
}

# ---------- append (manual, no quotes, UTF-8 without BOM, matches original style) ----------
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Add-Rows($path, $rows, $columns) {
    if ($rows.Count -eq 0) { return }
    $sb = New-Object System.Text.StringBuilder
    foreach ($r in $rows) {
        $vals = foreach ($col in $columns) { [string]$r.$col }
        [void]$sb.AppendLine(($vals -join ','))
    }
    # Append using raw bytes so we preserve UTF-8 without re-encoding existing content
    $bytes = $utf8NoBom.GetBytes($sb.ToString())
    $fs = [System.IO.File]::Open($path, [System.IO.FileMode]::Append)
    try { $fs.Write($bytes, 0, $bytes.Length) } finally { $fs.Close() }
}

Add-Rows (Join-Path $dst 'customers.csv') $newCustomers `
    @('customer_id','name','email','plan','segment','status','created_at','country')

Add-Rows (Join-Path $dst 'subscriptions.csv') $newSubs `
    @('subscription_id','customer_id','plan','status','mrr','start_date','end_date','billing_cycle')

Add-Rows (Join-Path $dst 'feedbacks.csv') $newFbs `
    @('feedback_id','customer_id','type','channel','sentiment','message','created_at','nps_score','resolved')

Write-Host ""
Write-Host "Appended:"
Write-Host "  $($newCustomers.Count) customers"
Write-Host "  $($newSubs.Count) subscriptions"
Write-Host "  $($newFbs.Count) feedbacks (including ~4% duplicates by design)"
Write-Host ""
Write-Host "Note: the synthetic rows preserve realistic quality issues:"
Write-Host "  - mixed id formats (CUST_ vs CUST-, sub_ vs SUB-)"
Write-Host "  - blank nps_score on non-NPS channels"
Write-Host "  - blank 'resolved' for some rows"
Write-Host "  - duplicate feedbacks (same customer + message + date)"
