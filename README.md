basic architecture : 

┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│  Producer   │──────▶│   Broker    │──────▶│  Consumer   │
│ (Publishes) │       │ (Manages    │       │ (Subscribed │
└─────────────┘       │  Partition) │       │  to Topic)  │
                      └─────────────┘       └─────────────┘
                           ▲
                           │ (Stores/Forwards)
                           ▼
                   ┌─────────────────┐
                   │  Topic:         │
                   │ "notifications" │
                   │  Partition 0    │
                   └─────────────────┘
