# Read-only onboarding helper for a NEW Telegram notification bot.
# Calls getMe/getUpdates only. Never sends a message, acknowledges an update via
# offset, deletes a webhook, stores a token, or changes Railway configuration.
# Run locally: powershell -NoProfile -File .\scripts\telegram-setup.ps1

[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-TelegramRead {
    param(
        [Parameter(Mandatory = $true)] [System.Net.Http.HttpClient] $Client,
        [Parameter(Mandatory = $true)] [string] $BotToken,
        [Parameter(Mandatory = $true)] [ValidateSet('getMe', 'getUpdates')] [string] $Method
    )

    $request = $null
    $response = $null
    $stream = $null
    $memory = $null
    $cancellation = New-Object System.Threading.CancellationTokenSource
    $cancellation.CancelAfter(15000)
    try {
        # The token was validated as a single safe URL path segment. No URL or
        # underlying exception is written to the console, including on failure.
        $request = New-Object System.Net.Http.HttpRequestMessage
        $request.Method = [System.Net.Http.HttpMethod]::Post
        $request.RequestUri = [Uri]('https://api.telegram.org/bot' + $BotToken + '/' + $Method)
        $payload = '{}'
        if ($Method -eq 'getUpdates') {
            # Do not supply offset or allowed_updates: this helper does not
            # acknowledge pending updates or change the bot's update filters.
            $payload = '{"timeout":0,"limit":100}'
        }
        $request.Content = New-Object System.Net.Http.StringContent(
            $payload, [System.Text.Encoding]::UTF8, 'application/json'
        )
        $response = $Client.SendAsync(
            $request,
            [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead,
            $cancellation.Token
        ).GetAwaiter().GetResult()
        if (-not $response.IsSuccessStatusCode) {
            $status = [int]$response.StatusCode
            if ($status -eq 401) {
                throw 'Telegram rejected the bot token (HTTP 401). Check it in BotFather.'
            }
            if ($status -eq 409) {
                throw 'Telegram returned HTTP 409. A webhook or another update consumer may be active; no settings were changed.'
            }
            throw ('Telegram request was rejected (HTTP ' + $status + ').')
        }

        # A new bot normally has only a few updates. Keep both the response body
        # and its read time bounded without printing any user's message content.
        $maximumBytes = 1048576
        if ($response.Content.Headers.ContentLength -gt $maximumBytes) {
            throw 'Telegram response is too large for this onboarding helper.'
        }
        $stream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
        $memory = New-Object System.IO.MemoryStream
        $buffer = New-Object byte[] 4096
        while ($true) {
            $count = $stream.ReadAsync($buffer, 0, $buffer.Length, $cancellation.Token).GetAwaiter().GetResult()
            if ($count -eq 0) { break }
            if (($memory.Length + $count) -gt $maximumBytes) {
                throw 'Telegram response is too large for this onboarding helper.'
            }
            $memory.Write($buffer, 0, $count)
        }
        $decoded = [System.Text.Encoding]::UTF8.GetString($memory.ToArray()) | ConvertFrom-Json -ErrorAction Stop
        if ($decoded.ok -ne $true) {
            throw 'Telegram did not acknowledge the onboarding request.'
        }
        return $decoded.result
    }
    catch {
        # Never echo $_, Exception.Message, ErrorDetails, RequestUri or response
        # bodies: HTTP exceptions may contain the token-bearing request URL.
        $safeMessages = @(
            'Telegram rejected the bot token (HTTP 401). Check it in BotFather.',
            'Telegram returned HTTP 409. A webhook or another update consumer may be active; no settings were changed.',
            'Telegram response is too large for this onboarding helper.',
            'Telegram did not acknowledge the onboarding request.'
        )
        $candidate = $_.Exception.Message
        if (($safeMessages -contains $candidate) -or ($candidate -match '^Telegram request was rejected \(HTTP [0-9]{3}\)\.$')) {
            throw $candidate
        }
        throw 'Telegram onboarding request failed or timed out. Token, URL and response were withheld.'
    }
    finally {
        if ($null -ne $memory) { $memory.Dispose() }
        if ($null -ne $stream) { $stream.Dispose() }
        if ($null -ne $response) { $response.Dispose() }
        if ($null -ne $request) { $request.Dispose() }
        $cancellation.Dispose()
    }
}

$secureToken = $null
$tokenPointer = [IntPtr]::Zero
$tokenText = $null
$client = $null
$handler = $null
$rng = $null
try {
    Add-Type -AssemblyName System.Net.Http
    Write-Host 'Create your own bot with the verified @BotFather using /newbot first.'
    Write-Host 'Paste the token only into this hidden local prompt. Do not send it to chat or commit it.'
    $secureToken = Read-Host -Prompt 'Telegram bot token' -AsSecureString
    $tokenPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureToken)
    $tokenText = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($tokenPointer)
    if ($tokenText -notmatch '^[0-9]+:[A-Za-z0-9_-]+$') {
        throw 'The bot token format is invalid; its value was withheld.'
    }
    $handler = New-Object System.Net.Http.HttpClientHandler
    $handler.AllowAutoRedirect = $false
    $client = New-Object System.Net.Http.HttpClient($handler)
    $client.Timeout = [TimeSpan]::FromSeconds(15)
    $bot = Invoke-TelegramRead -Client $client -BotToken $tokenText -Method getMe
    if (($bot.is_bot -ne $true) -or ([string]$bot.username -notmatch '^[A-Za-z0-9_]{1,64}$')) {
        throw 'Telegram did not return a valid bot identity.'
    }

    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    $challengeBytes = New-Object byte[] 16
    $rng.GetBytes($challengeBytes)
    $challenge = 'paper-' + [BitConverter]::ToString($challengeBytes).Replace('-', '').ToLowerInvariant()
    $startedAt = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    Write-Host ('Open @' + $bot.username + ' in Telegram, press Start, then send this exact one-time code in a PRIVATE chat:')
    Write-Host $challenge
    Write-Host 'This helper accepts only your recent private message matching that code. It will not send any message.'

    $foundChat = $null
    for ($attempt = 0; $attempt -lt 3; $attempt++) {
        $answer = Read-Host -Prompt 'Press Enter after sending the code, or type q to cancel'
        if ($answer -ceq 'q') { throw 'Onboarding cancelled; no token was saved.' }
        $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
        if (($now - $startedAt) -gt 600) {
            throw 'The one-time code expired. Run this helper again to create a new code.'
        }
        $updates = @(Invoke-TelegramRead -Client $client -BotToken $tokenText -Method getUpdates)
        $matchingChats = @()
        foreach ($update in $updates) {
            if ($null -eq $update -or -not ($update.PSObject.Properties.Name -contains 'message')) { continue }
            $message = $update.message
            if (-not ($message.PSObject.Properties.Name -contains 'text')) { continue }
            if ($message.text -cne $challenge) { continue }
            if ($message.chat.type -cne 'private' -or $message.from.is_bot -ne $false) { continue }
            $messageTime = [long]$message.date
            if ($messageTime -lt ($startedAt - 5) -or $messageTime -gt ($now + 5)) { continue }
            $chatId = 0L
            $senderId = 0L
            if (-not [long]::TryParse([string]$message.chat.id, [ref]$chatId)) { continue }
            if (-not [long]::TryParse([string]$message.from.id, [ref]$senderId)) { continue }
            if ($chatId -le 0 -or $chatId -ne $senderId) { continue }
            $matchingChats += $chatId
        }
        $distinct = @($matchingChats | Sort-Object -Unique)
        if ($distinct.Count -gt 1) {
            throw 'More than one private chat matched the code. No destination was selected.'
        }
        if ($distinct.Count -eq 1) {
            $foundChat = $distinct[0]
            break
        }
        Write-Host 'No matching recent private message was found. Check the bot and code, then try again.'
    }
    if ($null -eq $foundChat) {
        throw 'No chat was selected. Pending updates were not acknowledged; use a new dedicated bot if its update backlog is full.'
    }
    Write-Host 'Verified private destination. Copy the number below into TELEGRAM_CHAT_ID; no token or file was saved.'
    Write-Output $foundChat
}
catch {
    # Only static helper errors reach this boundary; suppress all unexpected
    # details because PowerShell/.NET errors can include a request URL.
    $safe = $_.Exception.Message
    if ($safe -match '^(Telegram (onboarding request failed|rejected the bot token|returned HTTP 409|request was rejected|response is too large|did not acknowledge|did not return a valid bot identity)|The bot token format is invalid|Onboarding cancelled|The one-time code expired|More than one private chat matched|No chat was selected)') {
        Write-Error $safe -ErrorAction Continue
    }
    else {
        Write-Error 'Telegram onboarding failed. Sensitive error details were withheld; no token was saved.' -ErrorAction Continue
    }
    exit 1
}
finally {
    if ($null -ne $client) { $client.Dispose() }
    if ($null -ne $handler) { $handler.Dispose() }
    if ($null -ne $rng) { $rng.Dispose() }
    if ($tokenPointer -ne [IntPtr]::Zero) { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($tokenPointer) }
    if ($null -ne $secureToken) { $secureToken.Dispose() }
    $tokenText = $null
}
