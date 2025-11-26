# Integration Tests

This directory contains integration tests for the auction game system.

## Running Tests

To run all integration tests:

```bash
cargo test --test integration_test
```

To run a specific test:

```bash
cargo test --test integration_test test_full_game_flow
cargo test --test integration_test test_opt_out_mechanism
cargo test --test integration_test test_timer_based_selling
cargo test --test integration_test test_multiple_cricketers_auction
```

## Test Scenarios

### Test 1: Full Game Flow (`test_full_game_flow`)
- Creates a game with player1
- Player2 and Player3 join the game
- Creator starts the game
- Players place bids on cricketers
- Verifies game state and WebSocket events

### Test 2: Opt-Out Mechanism (`test_opt_out_mechanism`)
- Creates a game with 3 players
- Starts the game
- One player places a bid
- Other players opt out
- Verifies that the cricketer is sold when n-1 players opt out

### Test 3: Timer-Based Selling (`test_timer_based_selling`)
- Creates a game with 2 players
- Starts the game
- Places a bid
- Verifies timer mechanism (would trigger after MAX_IDLE_TIME_IN_SECS)

### Test 4: Multiple Cricketers Auction (`test_multiple_cricketers_auction`)
- Creates a game with 2 players
- Starts the game
- Bids on first cricketer and triggers sale
- Waits for next cricketer
- Bids on second cricketer and triggers sale
- Verifies multiple auctions work correctly

## Test Server

Each test automatically starts a test server on port 3001 (to avoid conflicts with a running production server on 3000).

The tests use:
- HTTP client (reqwest) for API calls
- WebSocket client (tokio-tungstenite) for real-time events
- Automatic server startup and teardown

## Notes

- Tests run sequentially by default
- Each test waits for the server to be ready before making requests
- WebSocket connections are established to receive real-time game events
- The timer test mentions that in production, you might want to reduce MAX_IDLE_TIME_IN_SECS for faster testing

