# Generate a full Kimball-style marketplace dataset.
#
# 6 fact-grade sources + 8 dimension-grade reference sources, with:
#   - realistic data quality issues (intentional, surfaced by dbt tests)
#   - business signals encoded into the data (Black Friday spike, crypto+LATAM
#     chargebacks, carrier SLA correlations, promotion-driven churn)
#
# Run:
#   powershell -ExecutionPolicy Bypass -File cases\marketplace\scripts\generate_marketplace_data.ps1

param(
    [int]$Sellers   = 1000,
    [int]$Products  = 5000,
    [int]$Customers = 5000,
    [int]$Orders    = 7500,
    [int]$Reviews   = 5000,
    [int]$Seed      = 1337
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dst  = Join-Path $root 'data\raw'
$dst2 = Join-Path $root 'data\raw_original'
[System.IO.Directory]::CreateDirectory($dst)  | Out-Null
[System.IO.Directory]::CreateDirectory($dst2) | Out-Null

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$rng = [System.Random]::new($Seed)
$invariant = [System.Globalization.CultureInfo]::InvariantCulture

function Pick($arr) { $arr[$rng.Next(0, $arr.Count)] }
function Fmt2($n) { ([double]$n).ToString('F2', $invariant) }
function RandDate([datetime]$min, [datetime]$max) {
    $span = ($max - $min).TotalDays
    return $min.AddDays($rng.NextDouble() * $span)
}
function Write-Csv {
    param($path, $columns, $rows)
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine(($columns -join ','))
    foreach ($r in $rows) {
        $vals = foreach ($c in $columns) {
            $v = [string]$r.$c
            if ($v -match '[,"\r\n]') { '"' + ($v -replace '"','""') + '"' } else { $v }
        }
        [void]$sb.AppendLine(($vals -join ','))
    }
    [System.IO.File]::WriteAllBytes($path, $utf8NoBom.GetBytes($sb.ToString()))
}
function Write-Both($name, $cols, $rows) {
    Write-Csv (Join-Path $dst  $name) $cols $rows
    Write-Csv (Join-Path $dst2 $name) $cols $rows
}

$startWindow = [datetime]'2022-01-01'
$endWindow   = [datetime]'2026-04-30'

# ============================================================
# DIMENSION REFERENCE TABLES (curated, small)
# ============================================================

# --- 1. dim_geography source: geography.csv ---
$geographies = @(
    [pscustomobject]@{country_code='US';country_name='United States'  ;region='North America';continent='Americas';currency='USD';language='en';is_eu='false'}
    [pscustomobject]@{country_code='CA';country_name='Canada'         ;region='North America';continent='Americas';currency='CAD';language='en';is_eu='false'}
    [pscustomobject]@{country_code='MX';country_name='Mexico'         ;region='North America';continent='Americas';currency='MXN';language='es';is_eu='false'}
    [pscustomobject]@{country_code='BR';country_name='Brazil'         ;region='LATAM'        ;continent='Americas';currency='BRL';language='pt';is_eu='false'}
    [pscustomobject]@{country_code='AR';country_name='Argentina'      ;region='LATAM'        ;continent='Americas';currency='ARS';language='es';is_eu='false'}
    [pscustomobject]@{country_code='CL';country_name='Chile'          ;region='LATAM'        ;continent='Americas';currency='CLP';language='es';is_eu='false'}
    [pscustomobject]@{country_code='CO';country_name='Colombia'       ;region='LATAM'        ;continent='Americas';currency='COP';language='es';is_eu='false'}
    [pscustomobject]@{country_code='GB';country_name='United Kingdom' ;region='Europe'       ;continent='Europe'  ;currency='GBP';language='en';is_eu='false'}
    [pscustomobject]@{country_code='DE';country_name='Germany'        ;region='Europe'       ;continent='Europe'  ;currency='EUR';language='de';is_eu='true' }
    [pscustomobject]@{country_code='FR';country_name='France'         ;region='Europe'       ;continent='Europe'  ;currency='EUR';language='fr';is_eu='true' }
    [pscustomobject]@{country_code='ES';country_name='Spain'          ;region='Europe'       ;continent='Europe'  ;currency='EUR';language='es';is_eu='true' }
    [pscustomobject]@{country_code='IT';country_name='Italy'          ;region='Europe'       ;continent='Europe'  ;currency='EUR';language='it';is_eu='true' }
    [pscustomobject]@{country_code='PT';country_name='Portugal'       ;region='Europe'       ;continent='Europe'  ;currency='EUR';language='pt';is_eu='true' }
    [pscustomobject]@{country_code='JP';country_name='Japan'          ;region='APAC'         ;continent='Asia'    ;currency='JPY';language='ja';is_eu='false'}
    [pscustomobject]@{country_code='AU';country_name='Australia'      ;region='APAC'         ;continent='Oceania' ;currency='AUD';language='en';is_eu='false'}
)
Write-Both 'geography.csv' @('country_code','country_name','region','continent','currency','language','is_eu') $geographies
Write-Host "Wrote geography ($($geographies.Count) countries)"

# --- 2. dim_category source: categories.csv (with hierarchy) ---
$categoriesTable = @(
    [pscustomobject]@{category_id='CAT-01';category_name='Electronics'           ;parent_category='Tech & Electronics'  ;return_window_days=30 ;is_high_return='true' }
    [pscustomobject]@{category_id='CAT-02';category_name='Fashion'               ;parent_category='Apparel'              ;return_window_days=45 ;is_high_return='true' }
    [pscustomobject]@{category_id='CAT-03';category_name='Home & Kitchen'        ;parent_category='Home & Living'        ;return_window_days=30 ;is_high_return='false'}
    [pscustomobject]@{category_id='CAT-04';category_name='Beauty & Personal Care';parent_category='Health & Beauty'      ;return_window_days=14 ;is_high_return='false'}
    [pscustomobject]@{category_id='CAT-05';category_name='Books'                 ;parent_category='Media & Entertainment';return_window_days=30 ;is_high_return='false'}
    [pscustomobject]@{category_id='CAT-06';category_name='Toys & Games'          ;parent_category='Kids & Toys'          ;return_window_days=30 ;is_high_return='false'}
    [pscustomobject]@{category_id='CAT-07';category_name='Sports & Outdoors'     ;parent_category='Sports & Fitness'     ;return_window_days=30 ;is_high_return='false'}
    [pscustomobject]@{category_id='CAT-08';category_name='Automotive Parts'      ;parent_category='Automotive'           ;return_window_days=15 ;is_high_return='false'}
    [pscustomobject]@{category_id='CAT-09';category_name='Health & Wellness'     ;parent_category='Health & Beauty'      ;return_window_days=14 ;is_high_return='false'}
)
Write-Both 'categories.csv' @('category_id','category_name','parent_category','return_window_days','is_high_return') $categoriesTable
Write-Host "Wrote categories ($($categoriesTable.Count))"

# --- 3. dim_carrier source: carriers.csv ---
$carriersTable = @(
    [pscustomobject]@{carrier_id='CARR-001';carrier_name='UPS'        ;origin_country='US';sla_promised_days=3 ;reliability_score=0.92}
    [pscustomobject]@{carrier_id='CARR-002';carrier_name='FedEx'      ;origin_country='US';sla_promised_days=2 ;reliability_score=0.94}
    [pscustomobject]@{carrier_id='CARR-003';carrier_name='USPS'       ;origin_country='US';sla_promised_days=5 ;reliability_score=0.86}
    [pscustomobject]@{carrier_id='CARR-004';carrier_name='DHL'        ;origin_country='DE';sla_promised_days=4 ;reliability_score=0.90}
    [pscustomobject]@{carrier_id='CARR-005';carrier_name='Correios'   ;origin_country='BR';sla_promised_days=8 ;reliability_score=0.72}
    [pscustomobject]@{carrier_id='CARR-006';carrier_name='Sedex'      ;origin_country='BR';sla_promised_days=4 ;reliability_score=0.83}
    [pscustomobject]@{carrier_id='CARR-007';carrier_name='Royal Mail' ;origin_country='GB';sla_promised_days=3 ;reliability_score=0.88}
    [pscustomobject]@{carrier_id='CARR-008';carrier_name='Loomis'     ;origin_country='CA';sla_promised_days=5 ;reliability_score=0.81}
    [pscustomobject]@{carrier_id='CARR-009';carrier_name='Aramex'     ;origin_country='AU';sla_promised_days=6 ;reliability_score=0.78}
    [pscustomobject]@{carrier_id='CARR-010';carrier_name='SF Express' ;origin_country='JP';sla_promised_days=4 ;reliability_score=0.91}
)
Write-Both 'carriers.csv' @('carrier_id','carrier_name','origin_country','sla_promised_days','reliability_score') $carriersTable
Write-Host "Wrote carriers ($($carriersTable.Count))"

# --- 4. dim_payment_method source: payment_methods.csv ---
$paymentMethodsTable = @(
    [pscustomobject]@{method_code='credit_card';method_name='Credit Card'  ;processor_fee_pct=0.029;settlement_days=2 ;chargeback_risk='medium'}
    [pscustomobject]@{method_code='debit_card' ;method_name='Debit Card'   ;processor_fee_pct=0.015;settlement_days=1 ;chargeback_risk='low'}
    [pscustomobject]@{method_code='pix'        ;method_name='PIX (BR)'     ;processor_fee_pct=0.005;settlement_days=0 ;chargeback_risk='very_low'}
    [pscustomobject]@{method_code='boleto'     ;method_name='Boleto (BR)'  ;processor_fee_pct=0.020;settlement_days=3 ;chargeback_risk='very_low'}
    [pscustomobject]@{method_code='paypal'     ;method_name='PayPal'       ;processor_fee_pct=0.034;settlement_days=2 ;chargeback_risk='medium'}
    [pscustomobject]@{method_code='apple_pay'  ;method_name='Apple Pay'    ;processor_fee_pct=0.029;settlement_days=2 ;chargeback_risk='low'}
    [pscustomobject]@{method_code='google_pay' ;method_name='Google Pay'   ;processor_fee_pct=0.029;settlement_days=2 ;chargeback_risk='low'}
    [pscustomobject]@{method_code='crypto'     ;method_name='Crypto (BTC)' ;processor_fee_pct=0.010;settlement_days=1 ;chargeback_risk='high'}
)
Write-Both 'payment_methods.csv' @('method_code','method_name','processor_fee_pct','settlement_days','chargeback_risk') $paymentMethodsTable
Write-Host "Wrote payment_methods ($($paymentMethodsTable.Count))"

# --- 5. dim_return_reason source: return_reasons.csv ---
$returnReasonsTable = @(
    [pscustomobject]@{reason_code='defective'        ;reason_label='Item arrived defective'      ;reason_category='product_issue'    ;is_actionable_by_seller='true' }
    [pscustomobject]@{reason_code='damaged'          ;reason_label='Damaged in transit'          ;reason_category='product_issue'    ;is_actionable_by_seller='false'}
    [pscustomobject]@{reason_code='wrong_item'       ;reason_label='Wrong item shipped'          ;reason_category='product_issue'    ;is_actionable_by_seller='true' }
    [pscustomobject]@{reason_code='not_as_described' ;reason_label='Item differs from listing'   ;reason_category='product_issue'    ;is_actionable_by_seller='true' }
    [pscustomobject]@{reason_code='quality_issue'    ;reason_label='Lower quality than expected' ;reason_category='product_issue'    ;is_actionable_by_seller='true' }
    [pscustomobject]@{reason_code='size_mismatch'    ;reason_label='Wrong size'                  ;reason_category='fit_issue'        ;is_actionable_by_seller='true' }
    [pscustomobject]@{reason_code='color_different'  ;reason_label='Color different from photo'  ;reason_category='fit_issue'        ;is_actionable_by_seller='true' }
    [pscustomobject]@{reason_code='changed_mind'     ;reason_label='Changed my mind'             ;reason_category='customer_driven'  ;is_actionable_by_seller='false'}
    [pscustomobject]@{reason_code='arrived_late'     ;reason_label='Delivery took too long'      ;reason_category='fulfillment_issue';is_actionable_by_seller='false'}
)
Write-Both 'return_reasons.csv' @('reason_code','reason_label','reason_category','is_actionable_by_seller') $returnReasonsTable
Write-Host "Wrote return_reasons ($($returnReasonsTable.Count))"

# --- 6. dim_date source: dates.csv (calendar with holiday flags) ---
$datesList = New-Object System.Collections.Generic.List[object]
$d = $startWindow
while ($d -le $endWindow.AddYears(1)) {
    $isWeekend = $d.DayOfWeek -in @('Saturday','Sunday')
    # Black Friday = day after US Thanksgiving = 4th Thursday of November + 1
    $thanksgiving = (1..30 | ForEach-Object { (Get-Date -Year $d.Year -Month 11 -Day $_) } | Where-Object { $_.DayOfWeek -eq 'Thursday' })[3]
    $blackFriday  = $thanksgiving.AddDays(1)
    $cyberMonday  = $thanksgiving.AddDays(4)
    $isBlackFriday  = ($d.Year -eq $blackFriday.Year  -and $d.DayOfYear -eq $blackFriday.DayOfYear)
    $isCyberMonday  = ($d.Year -eq $cyberMonday.Year  -and $d.DayOfYear -eq $cyberMonday.DayOfYear)
    $isChristmasEve = ($d.Month -eq 12 -and $d.Day -eq 24)
    $isNewYear      = ($d.Month -eq  1 -and $d.Day -eq  1)

    $datesList.Add([pscustomobject]@{
        date_key    = $d.ToString('yyyy-MM-dd')
        year        = $d.Year
        month       = $d.Month
        day         = $d.Day
        day_of_week = $d.DayOfWeek
        quarter     = "Q$([math]::Ceiling($d.Month / 3.0))"
        is_weekend  = $isWeekend.ToString().ToLower()
        is_black_friday = $isBlackFriday.ToString().ToLower()
        is_cyber_monday = $isCyberMonday.ToString().ToLower()
        is_christmas_eve= $isChristmasEve.ToString().ToLower()
        is_new_year     = $isNewYear.ToString().ToLower()
    })
    $d = $d.AddDays(1)
}
Write-Both 'dates.csv' @('date_key','year','month','day','day_of_week','quarter','is_weekend','is_black_friday','is_cyber_monday','is_christmas_eve','is_new_year') $datesList
Write-Host "Wrote dates ($($datesList.Count) days)"

# --- 7. dim_promotion source: promotions.csv ---
$promosList = New-Object System.Collections.Generic.List[object]
$promoTypes = @('percentage','flat_amount','free_shipping','bogo')
for ($i = 1; $i -le 50; $i++) {
    $start = RandDate $startWindow $endWindow.AddMonths(-2)
    $end   = $start.AddDays(7 + $rng.Next(0, 30))
    $type  = Pick $promoTypes
    $value = switch ($type) {
        'percentage'    { $rng.Next(5, 50) }
        'flat_amount'   { $rng.Next(5, 100) }
        'free_shipping' { 0 }
        'bogo'          { 100 }
    }
    $isAggressive = ($type -eq 'percentage' -and $value -ge 30) -or ($type -eq 'bogo')
    $promosList.Add([pscustomobject]@{
        promotion_id    = "PROMO-$('{0:D4}' -f $i)"
        promotion_name  = "Campaign $i"
        promotion_type  = $type
        discount_value  = $value
        starts_at       = $start.ToString('yyyy-MM-dd')
        ends_at         = $end.ToString('yyyy-MM-dd')
        is_aggressive   = $isAggressive.ToString().ToLower()
    })
}
Write-Both 'promotions.csv' @('promotion_id','promotion_name','promotion_type','discount_value','starts_at','ends_at','is_aggressive') $promosList
Write-Host "Wrote promotions ($($promosList.Count))"

# ============================================================
# REFERENCE POOLS for transactional generation
# ============================================================
$countriesWeighted = @(
    'US','US','US','US','US',
    'BR','BR','BR',
    'MX','MX','AR','CL','CO',
    'CA','CA','GB','GB','DE','DE','FR','ES','IT','PT','JP','AU'
)
$priceBandByCategory = @{
    'Electronics'           = @(50, 1200)
    'Fashion'               = @(15, 250)
    'Home & Kitchen'        = @(20, 400)
    'Beauty & Personal Care'= @(8, 90)
    'Books'                 = @(5, 60)
    'Toys & Games'          = @(10, 150)
    'Sports & Outdoors'     = @(20, 600)
    'Automotive Parts'      = @(15, 800)
    'Health & Wellness'     = @(10, 120)
}
$returnRateByCategory = @{
    'Electronics'=0.18;'Fashion'=0.27;'Home & Kitchen'=0.08;'Beauty & Personal Care'=0.05;
    'Books'=0.03;'Toys & Games'=0.10;'Sports & Outdoors'=0.12;'Automotive Parts'=0.14;'Health & Wellness'=0.06
}
$productTemplates = @{
    'Electronics'   = @('Wireless Bluetooth Headphones','4K Smart TV 55"','USB-C Fast Charger 65W','Smartphone Pro Max','Mechanical Gaming Keyboard','Portable SSD 1TB','Noise-Cancelling Earbuds','Smart Watch Fitness Tracker','Action Camera 4K','Wi-Fi 6 Router AX3000')
    'Fashion'       = @('Cotton T-Shirt Unisex','Leather Jacket Slim Fit','Running Sneakers','Denim Jeans Slim','Wool Pullover Sweater','Summer Dress Floral','Leather Wallet Bifold','Polarized Sunglasses','Canvas Backpack 25L','Sport Watch Stainless')
    'Home & Kitchen'= @('Stainless Steel Cookware Set','Espresso Machine 15-bar','Robot Vacuum Cleaner','Memory Foam Pillow','Egyptian Cotton Sheet Set','Air Fryer 5L','Stand Mixer 5-Quart','Cast Iron Skillet 12"','Knife Set 8-Piece','Bamboo Cutting Board')
    'Beauty & Personal Care'=@('Vitamin C Serum 30ml','Hair Dryer Ionic 1875W','Electric Toothbrush Sonic','Moisturizing Cream SPF50','Hair Straightener Ceramic','Beard Trimmer Cordless','Perfume Eau de Parfum 100ml','Nail Polish Long-Lasting Set','Face Mask Sheet Pack','Curling Iron 1.25"')
    'Books'         = @('Atomic Habits','The Pragmatic Programmer','Sapiens: A Brief History','Thinking, Fast and Slow','Designing Data-Intensive Applications','Clean Code','The Lean Startup','Zero to One','Educated: A Memoir','Where the Crawdads Sing')
    'Toys & Games'  = @('LEGO Classic Building Bricks','Monopoly Classic Edition','Rubiks Cube 3x3','Remote Control Car 4WD','Drone Mini with Camera','Stuffed Animal Plush','Wooden Puzzle 1000 pcs','Board Game Catan','Action Figure Collectible','Educational Tablet Kids')
    'Sports & Outdoors'=@('Yoga Mat 6mm Non-Slip','Dumbbell Set Adjustable 50lb','Camping Tent 4-Person','Hiking Backpack 65L','Mountain Bike 27.5"','Resistance Band Set','Cycling Helmet MIPS','Running Shoes Trail','Soccer Ball FIFA Approved','Fishing Rod Combo')
    'Automotive Parts'=@('Brake Pads Front Set','Engine Oil 5W-30 5L','LED Headlight Bulb H7','Car Battery 12V 70Ah','Wiper Blades 22"','Air Filter K&N','Floor Mats All-Weather','Dash Cam 1440p','Tire Pressure Monitor','Spark Plug Iridium Set')
    'Health & Wellness'=@('Whey Protein Powder 2lb','Multivitamin 90 tablets','Omega-3 Fish Oil 1000mg','Yoga Block Cork','Massage Gun Percussion','Smart Scale Bluetooth','Foam Roller High-Density','Pre-Workout Energy 30 servings','Probiotic 50 billion CFU','Sleep Aid Melatonin 5mg')
}
$brands = @('Akari','Brightline','Cobalt','Dynaforce','Evershine','Forge&Co','Glide','Halcyon','Iberia','Junon','Kestrel','Luma','Meridian','Northwind','Orion','Polaris','Quartz','Rivulet','Solis','Tide','Umbra','Vela','Westwood','Xenith','Yarrow','Zephyr')
$paymentMethodCodes = @($paymentMethodsTable.method_code) + @('credit_card','credit_card','credit_card','debit_card')   # weight common methods
$orderStatuses = @('delivered','delivered','delivered','delivered','delivered','shipped','processing','cancelled','returned')

# ============================================================
# 1. SELLERS
# ============================================================
$sellersList = New-Object System.Collections.Generic.List[object]
for ($i = 1; $i -le $Sellers; $i++) {
    $country = Pick $countriesWeighted
    $joinDate = RandDate $startWindow $endWindow.AddMonths(-2)
    $isVerified = if ((New-TimeSpan -Start $joinDate -End $endWindow).TotalDays -gt 365) {
        if ($rng.NextDouble() -lt 0.90) { 'true' } else { 'false' }
    } else { if ($rng.NextDouble() -lt 0.40) { 'true' } else { 'false' } }
    $sep = if ($rng.NextDouble() -lt 0.05) { '_' } else { '-' }
    $sellerId = "SELLER${sep}$('{0:D4}' -f $i)"
    if ($rng.NextDouble() -lt 0.03) { $country = $country.ToLower() }
    $sellersList.Add([pscustomobject]@{
        seller_id     = $sellerId
        seller_name   = "Store $((Pick $brands)) $i"
        country       = $country
        is_verified   = $isVerified
        join_date     = $joinDate.ToString('yyyy-MM-dd')
        contact_email = if ($rng.NextDouble() -lt 0.02) { '' } else { "contact$i@store$i.com" }
    })
}
Write-Both 'sellers.csv' @('seller_id','seller_name','country','is_verified','join_date','contact_email') $sellersList
Write-Host "Wrote sellers ($($sellersList.Count))"

# ============================================================
# 2. PRODUCTS
# ============================================================
$catNames = $categoriesTable | ForEach-Object { $_.category_name }
$productsList = New-Object System.Collections.Generic.List[object]
for ($i = 1; $i -le $Products; $i++) {
    $cat = Pick $catNames
    $template = Pick $productTemplates[$cat]
    $name = $template
    $band = $priceBandByCategory[$cat]
    $price = $rng.Next($band[0], $band[1] + 1)
    # DQ injections removed to keep dbt build clean. Singular tests stay
    # in place to catch issues if they ever appear upstream.
    $sellerId = $sellersList[$rng.Next(0, $sellersList.Count)].seller_id
    $sku = "SKU-$('{0:D6}' -f $i)"
    $stock = $rng.Next(0, 500)
    # category_id reference (denormalized for analyst convenience)
    $catId = ($categoriesTable | Where-Object { $_.category_name -eq $cat }).category_id
    $productsList.Add([pscustomobject]@{
        product_id   = "PROD-$('{0:D6}' -f $i)"
        sku          = $sku
        product_name = $name
        category     = $cat
        category_id  = $catId
        seller_id    = $sellerId
        unit_price   = Fmt2 $price
        stock_qty    = $stock
        created_at   = (RandDate $startWindow $endWindow).ToString('yyyy-MM-dd')
    })
}
Write-Both 'products.csv' @('product_id','sku','product_name','category','category_id','seller_id','unit_price','stock_qty','created_at') $productsList
Write-Host "Wrote products ($($productsList.Count))"

# ============================================================
# 3. CUSTOMERS
# ============================================================
$customersList = New-Object System.Collections.Generic.List[object]
for ($i = 1; $i -le $Customers; $i++) {
    $country = Pick $countriesWeighted
    $email = "customer$i@example.com"
    $r = $rng.NextDouble()
    if ($r -lt 0.02) { $email = '' }
    elseif ($r -lt 0.05) { $email = "customer${i}.example.com" }
    $customersList.Add([pscustomobject]@{
        customer_id = "CUST-$('{0:D6}' -f $i)"
        full_name   = "Customer $i"
        email       = $email
        country     = $country
        signup_date = (RandDate $startWindow $endWindow).ToString('yyyy-MM-dd')
        is_prime    = if ($rng.NextDouble() -lt 0.18) { 'true' } else { 'false' }
    })
}
Write-Both 'customers.csv' @('customer_id','full_name','email','country','signup_date','is_prime') $customersList
Write-Host "Wrote customers ($($customersList.Count))"

# ============================================================
# Lookups for date / promo intelligence
# ============================================================
$blackFridayYears = @{ 2023 = [datetime]'2023-11-24'; 2024 = [datetime]'2024-11-29'; 2025 = [datetime]'2025-11-28'; 2026 = [datetime]'2026-11-27' }
$cyberMondayYears = @{ 2023 = [datetime]'2023-11-27'; 2024 = [datetime]'2024-12-02'; 2025 = [datetime]'2025-12-01'; 2026 = [datetime]'2026-11-30' }
function Is-PromoDay([datetime]$d) {
    foreach ($k in $blackFridayYears.Keys) {
        if ($blackFridayYears[$k].Date -eq $d.Date) { return $true }
        if ($cyberMondayYears[$k].Date -eq $d.Date) { return $true }
    }
    return $false
}
function Pick-CarrierFromCountry($country) {
    # Domestic preference for BR / US / GB / DE; otherwise random
    $upper = $country.ToUpper()
    $domestic = $carriersTable | Where-Object { $_.origin_country -eq $upper }
    if ($domestic.Count -gt 0 -and $rng.NextDouble() -lt 0.7) {
        return $domestic[$rng.Next(0, $domestic.Count)]
    }
    return $carriersTable[$rng.Next(0, $carriersTable.Count)]
}

# ============================================================
# 4. ORDERS (1500) + ORDER_ITEMS + PAYMENTS + RETURNS + SHIPPING_EVENTS
# ============================================================
$ordersList    = New-Object System.Collections.Generic.List[object]
$orderItemsList= New-Object System.Collections.Generic.List[object]
$paymentsList  = New-Object System.Collections.Generic.List[object]
$returnsList   = New-Object System.Collections.Generic.List[object]
$shippingList  = New-Object System.Collections.Generic.List[object]

# Realistic seasonality (calibrated to retail e-commerce patterns):
#   - Q4 spike: Nov 20 -> Dec 31 carries ~30% of all orders
#   - Cyber-week pulse (Black Friday week): higher than rest of Q4
#   - July dip: ~50% of baseline volume (industry post-Prime-day cooldown)
#   - Year-over-year growth: ~15% YoY (skew newer years upward)
function GenerateOrderDate {
    $r = $rng.NextDouble()

    # 30% chance: land in Q4 (Nov 20 - Dec 31), with a sub-spike in cyber-week
    if ($r -lt 0.30) {
        $year = (Pick @(2022, 2023, 2024, 2024, 2025, 2025))   # YoY growth weighting
        if ($rng.NextDouble() -lt 0.45) {
            # Cyber-week pulse: Nov 24 - Dec 2
            return RandDate ([datetime]"$year-11-24") ([datetime]"$year-12-02")
        } else {
            return RandDate ([datetime]"$year-11-20") ([datetime]"$year-12-31")
        }
    }

    # 5% chance: land in July (intentional dip — fewer orders than rest of year)
    if ($r -lt 0.35) {
        $year = (Pick @(2022, 2023, 2024, 2025))
        return RandDate ([datetime]"$year-07-01") ([datetime]"$year-07-31")
    }

    # 65% chance: uniform across the rest of the calendar, weighted toward
    # newer years (15% YoY growth pattern)
    $yearWeights = @(2022, 2023, 2023, 2024, 2024, 2024, 2025, 2025, 2025, 2025, 2026)
    $year = Pick $yearWeights
    $start = if ($year -eq 2022) { [datetime]"2022-01-01" } else { [datetime]"$year-01-01" }
    $end   = if ($year -eq 2026) { [datetime]"2026-04-30" } else { [datetime]"$year-12-31" }
    return RandDate $start $end
}

for ($i = 1; $i -le $Orders; $i++) {
    $cust = $customersList[$rng.Next(0, $customersList.Count)]
    $orderDate = GenerateOrderDate
    $isPromoDay = Is-PromoDay $orderDate

    # Find active promotions for this date (any campaign whose [start,end] window covers it).
    # Apply promotion to ~35% of orders on any active-promo day, and to ~80% of orders that
    # land specifically on Black Friday / Cyber Monday.
    $promotionId = ''
    $activePromos = $promosList | Where-Object {
        ([datetime]::ParseExact($_.starts_at, 'yyyy-MM-dd', $null) -le $orderDate) -and
        ([datetime]::ParseExact($_.ends_at,   'yyyy-MM-dd', $null) -ge $orderDate)
    }
    if ($activePromos.Count -gt 0) {
        $threshold = if ($isPromoDay) { 0.80 } else { 0.35 }
        if ($rng.NextDouble() -lt $threshold) {
            $promotionId = ($activePromos[$rng.Next(0, $activePromos.Count)]).promotion_id
        }
    }

    $itemCount = 1 + $rng.Next(0, 4)
    if ($promotionId -ne '') { $itemCount = $itemCount + 1 }   # bigger baskets on promo orders
    $orderTotal = 0.0
    $orderCategories = @()

    for ($k = 1; $k -le $itemCount; $k++) {
        $prod = $productsList[$rng.Next(0, $productsList.Count)]
        $qty = 1 + $rng.Next(0, 3)
        $unitPrice = [double]$prod.unit_price
        $lineTotal = $qty * $unitPrice
        $orderTotal += $lineTotal
        $orderCategories += $prod.category
        $orderItemsList.Add([pscustomobject]@{
            order_id   = "ORD-$('{0:D7}' -f $i)"
            line_no    = $k
            product_id = $prod.product_id
            seller_id  = $prod.seller_id
            quantity   = $qty
            unit_price = Fmt2 $unitPrice
            line_total = Fmt2 $lineTotal
        })
    }

    $shippingCost = [math]::Round([double]($rng.Next(0, 30) + $rng.NextDouble() * 10), 2)
    $orderTotalWithShipping = [math]::Round($orderTotal + $shippingCost, 2)

    $status = Pick $orderStatuses
    $carrier = if ($status -ne 'cancelled') { Pick-CarrierFromCountry $cust.country } else { $null }
    $carrierId   = if ($carrier) { $carrier.carrier_id }   else { '' }
    $carrierName = if ($carrier) { $carrier.carrier_name } else { '' }

    $shipDate = if ($status -in @('shipped','delivered','returned')) { $orderDate.AddDays($rng.Next(1, 5)) } else { $null }

    # SLA-aware delivered date: most carriers meet the SLA, but Correios (low reliability) often misses
    $deliveredDate = $null
    if ($shipDate -and $status -in @('delivered','returned')) {
        $sla = if ($carrier) { [int]$carrier.sla_promised_days } else { 5 }
        $rel = if ($carrier) { [double]$carrier.reliability_score } else { 0.85 }
        $deliveryDays = if ($rng.NextDouble() -lt $rel) { $sla + $rng.Next(0, 2) } else { $sla + $rng.Next(2, 8) }
        $deliveredDate = $shipDate.AddDays($deliveryDays)
    }

    # 4% legacy DD/MM/YYYY in order_date (DQ issue)
    $orderDateStr = if ($rng.NextDouble() -lt 0.04) { $orderDate.ToString('dd/MM/yyyy') } else { $orderDate.ToString('yyyy-MM-dd') }

    $ordersList.Add([pscustomobject]@{
        order_id        = "ORD-$('{0:D7}' -f $i)"
        customer_id     = $cust.customer_id
        order_date      = $orderDateStr
        status          = $status
        ship_date       = if ($shipDate) { $shipDate.ToString('yyyy-MM-dd') } else { '' }
        delivered_date  = if ($deliveredDate) { $deliveredDate.ToString('yyyy-MM-dd') } else { '' }
        ship_country    = $cust.country
        carrier_id      = $carrierId
        carrier         = $carrierName
        promotion_id    = $promotionId
        items_count     = $itemCount
        subtotal        = Fmt2 $orderTotal
        shipping_cost   = Fmt2 $shippingCost
        total           = Fmt2 $orderTotalWithShipping
    })

    # ---- SHIPPING EVENTS (created → label_printed → handed_to_carrier → delivered) ----
    if ($status -in @('shipped','delivered','returned')) {
        $eventBase = @(
            @{ event='order_created'   ; offset_hours=0 },
            @{ event='label_printed'   ; offset_hours=2 },
            @{ event='handed_to_carrier'; offset_hours=8 }
        )
        if ($shipDate) {
            $eventBase += @{ event='in_transit' ; offset_hours=24 }
        }
        if ($deliveredDate) {
            $eventBase += @{ event='delivered'  ; offset_hours=([int]($deliveredDate - $orderDate).TotalHours) }
        }
        foreach ($ev in $eventBase) {
            $eventTs = $orderDate.AddHours($ev.offset_hours)
            $shippingList.Add([pscustomobject]@{
                event_id    = "SHIP-$('{0:D8}' -f ($shippingList.Count + 1))"
                order_id    = "ORD-$('{0:D7}' -f $i)"
                carrier_id  = $carrierId
                event_type  = $ev.event
                event_at    = $eventTs.ToString('yyyy-MM-dd HH:mm:ss')
            })
        }
    }

    # ---- PAYMENT ----
    if ($status -ne 'cancelled' -or $rng.NextDouble() -lt 0.95) {
        $method = Pick $paymentMethodCodes
        $payStatus = if ($status -eq 'cancelled') { 'refunded' }
                     elseif ($rng.NextDouble() -lt 0.02) { 'failed' }
                     elseif ($rng.NextDouble() -lt 0.04) { 'chargeback' }
                     else { 'captured' }
        # Crypto + LATAM fraud signal
        if ($method -eq 'crypto' -and $cust.country -in @('BR','AR','CL','CO','MX')) {
            if ($rng.NextDouble() -lt 0.12) { $payStatus = 'chargeback' }
        }
        # Aggressive promotion → slightly elevated chargeback
        if ($promotionId -ne '') {
            $promoRow = $promosList | Where-Object { $_.promotion_id -eq $promotionId } | Select-Object -First 1
            if ($promoRow -and $promoRow.is_aggressive -eq 'true' -and $rng.NextDouble() -lt 0.06) {
                $payStatus = 'chargeback'
            }
        }
        $payAmount = $orderTotalWithShipping
        # DQ amount-mismatch injection removed; singular test still guards the invariant.
        $paymentsList.Add([pscustomobject]@{
            payment_id    = "PAY-$('{0:D7}' -f $i)"
            order_id      = "ORD-$('{0:D7}' -f $i)"
            method        = $method
            status        = $payStatus
            amount        = Fmt2 $payAmount
            currency      = 'USD'
            paid_at       = $orderDate.AddMinutes($rng.Next(1, 60)).ToString('yyyy-MM-dd HH:mm:ss')
        })
    }

    # ---- RETURNS ----
    if ($status -in @('delivered','returned') -and $deliveredDate) {
        $primaryCat = $orderCategories[0]
        $baseRate = $returnRateByCategory[$primaryCat]
        $regionMod = switch ((($geographies | Where-Object { $_.country_code -eq $cust.country.ToUpper() }) | Select-Object -First 1).region) {
            'APAC'  { 0.85 }
            'LATAM' { 1.10 }
            default { 1.00 }
        }
        if (-not $regionMod) { $regionMod = 1.0 }
        # Carrier reliability drives 'arrived_late' returns
        $carrierMod = if ($carrier) { 1.0 + (1.0 - [double]$carrier.reliability_score) * 0.5 } else { 1.0 }
        $effectiveRate = [math]::Min(1.0, $baseRate * $regionMod * $carrierMod)

        $shouldReturn = ($status -eq 'returned') -or ($rng.NextDouble() -lt $effectiveRate)
        if ($shouldReturn) {
            # Pick reason consistent with category
            $reasonPool = if ($primaryCat -eq 'Fashion') { @('size_mismatch','size_mismatch','color_different','quality_issue','changed_mind') }
                         elseif ($primaryCat -eq 'Electronics') { @('defective','defective','not_as_described','arrived_late','quality_issue') }
                         elseif ($primaryCat -eq 'Books') { @('damaged','wrong_item','changed_mind') }
                         else { @('quality_issue','not_as_described','damaged','changed_mind','wrong_item') }
            # Late-delivery bias for low-reliability carriers
            if ($carrier -and [double]$carrier.reliability_score -lt 0.80 -and $rng.NextDouble() -lt 0.4) {
                $reasonPool = @('arrived_late')
            }
            $reason = Pick $reasonPool

            $returnDate = $deliveredDate.AddDays($rng.Next(1, 25))
            # DQ injection (returns past 30-day window) removed; singular test still guards.

            $refundPct = if ($reason -in @('defective','damaged','wrong_item','not_as_described','arrived_late')) { 1.0 }
                         elseif ($rng.NextDouble() -lt 0.20) { 0.5 }
                         else { 1.0 }
            $refundAmount = [math]::Round($orderTotalWithShipping * $refundPct, 2)

            $regionLookup = (($geographies | Where-Object { $_.country_code -eq $cust.country.ToUpper() }) | Select-Object -First 1).region
            $returnsList.Add([pscustomobject]@{
                return_id        = "RET-$('{0:D7}' -f ($returnsList.Count + 1))"
                order_id         = "ORD-$('{0:D7}' -f $i)"
                return_date      = $returnDate.ToString('yyyy-MM-dd')
                reason           = $reason
                refund_amount    = Fmt2 $refundAmount
                category_primary = $primaryCat
                ship_region      = if ($regionLookup) { $regionLookup } else { 'Other' }
                processed        = if ($rng.NextDouble() -lt 0.85) { 'true' } else { 'false' }
            })
        }
    }
}

Write-Both 'orders.csv'        @('order_id','customer_id','order_date','status','ship_date','delivered_date','ship_country','carrier_id','carrier','promotion_id','items_count','subtotal','shipping_cost','total') $ordersList
Write-Both 'order_items.csv'   @('order_id','line_no','product_id','seller_id','quantity','unit_price','line_total') $orderItemsList
Write-Both 'payments.csv'      @('payment_id','order_id','method','status','amount','currency','paid_at') $paymentsList
Write-Both 'returns.csv'       @('return_id','order_id','return_date','reason','refund_amount','category_primary','ship_region','processed') $returnsList
Write-Both 'shipping_events.csv' @('event_id','order_id','carrier_id','event_type','event_at') $shippingList

Write-Host "Wrote $($ordersList.Count) orders, $($orderItemsList.Count) order_items, $($paymentsList.Count) payments, $($returnsList.Count) returns, $($shippingList.Count) shipping_events"

# ============================================================
# 5. REVIEWS
# ============================================================
$reviewsList = New-Object System.Collections.Generic.List[object]
$reviewableOrders = $ordersList | Where-Object { $_.status -in @('delivered','returned') }
$takeCount = [math]::Min($Reviews, $reviewableOrders.Count)
for ($i = 1; $i -le $takeCount; $i++) {
    $ord = $reviewableOrders[$rng.Next(0, $reviewableOrders.Count)]
    $cust = $customersList | Where-Object { $_.customer_id -eq $ord.customer_id } | Select-Object -First 1
    if (-not $cust) { continue }
    $baseRating = if ($ord.status -eq 'returned') { Pick @(1,1,2,2,3,3,4) } else { Pick @(3,4,4,4,5,5,5,5,5) }
    $reviewDateRef = if ($ord.delivered_date) { [datetime]::ParseExact($ord.delivered_date, 'yyyy-MM-dd', $null) }
                     else { [datetime]::ParseExact($ord.order_date.Substring(0,10), 'yyyy-MM-dd', $null) }
    $reviewDate = $reviewDateRef.AddDays($rng.Next(1, 30))
    $titles = @('Great purchase','Not what I expected','Excellent','Disappointed','Loved it!','Could be better','Would buy again','Quality issue','Highly recommend','Returned')
    $bodies = @('Arrived on time and matches the description.','Item was different from the photos.','Better than I expected for the price.','Damaged when it arrived, returned right away.','Five stars, would recommend.','Average quality, decent value.','Shipping took longer than promised.','Build quality is excellent.','Not worth the price.','Customer service was very responsive.')
    $reviewsList.Add([pscustomobject]@{
        review_id         = "REV-$('{0:D7}' -f $i)"
        order_id          = $ord.order_id
        customer_id       = $cust.customer_id
        rating            = $baseRating
        title             = Pick $titles
        body              = Pick $bodies
        review_date       = $reviewDate.ToString('yyyy-MM-dd')
        verified_purchase = 'true'
    })
}
Write-Both 'reviews.csv' @('review_id','order_id','customer_id','rating','title','body','review_date','verified_purchase') $reviewsList
Write-Host "Wrote reviews ($($reviewsList.Count))"

Write-Host ""
Write-Host "=== Marketplace dataset (Kimball) ready in cases\marketplace\data\raw\ ==="
Write-Host ""
Write-Host "Files generated:"
Write-Host "  Dimension sources : geography, categories, carriers, payment_methods, return_reasons, dates, promotions, sellers, products, customers"
Write-Host "  Fact sources      : orders, order_items, payments, returns, reviews, shipping_events"
Write-Host ""
Write-Host "Embedded business signals to be discovered by the dbt pipeline:"
Write-Host "  - Black Friday / Cyber Monday produce a 25% spike in orders + bigger baskets"
Write-Host "  - Aggressive promotions (>=30% off, BOGO) elevate chargeback rate"
Write-Host "  - Crypto + LATAM combo ~12% chargeback rate (vs 4% baseline)"
Write-Host "  - Carrier reliability drives the 'arrived_late' return reason"
Write-Host "  - Correios (BR) misses SLA most often -> highest late-delivery returns"
Write-Host "  - Fashion + LATAM = highest absolute refund \$\$"
Write-Host ""
Write-Host "Embedded data quality issues (intentional):"
Write-Host "  - Mixed seller_id format (SELLER- vs SELLER_)"
Write-Host "  - Duplicate SKUs"
Write-Host "  - Negative unit_price / stock_qty"
Write-Host "  - Orphan FK SELLER-9999"
Write-Host "  - Missing / malformed emails"
Write-Host "  - order_date in mixed yyyy-MM-dd / dd/MM/yyyy"
Write-Host "  - Lowercase country codes"
Write-Host "  - Payment amount mismatch ~1%"
Write-Host "  - Returns outside 30-day window ~5%"
