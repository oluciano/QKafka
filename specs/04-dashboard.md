# Specification 04: Embedded Telemetry & Lag Dashboard

This specification defines the design, visual structure, and api contracts for the **qKafka** Telemetry Dashboard (Fase 4).

---

## 1. Architectural Design

The Dashboard is embedded directly into the host ASP.NET Core application as middleware. It has **zero external infrastructure dependencies**, communicating with the broker and database via read-only contracts.

```
[ Developer Browser ] ◄──── (HTML, CSS, JS, SSE)
        │
        ▼ (HTTP Requests)
  [ qKafka Dashboard Middleware ]
        │
        ├─► Poll Broker Lag (Confluent.Kafka Admin API)
        ├─► Fetch Saga States & Outbox (IDashboardStorage)
        └─► Stream live consumption events (In-Memory Activity Listeners)
```

---

## 2. Visual Structure & Aesthetics

To provide a premium developer experience, the dashboard will follow a modern design system:
* **Typography:** Inter or Roboto Google fonts.
* **Palette:** Sleek dark mode as default (Deep grays, neon purple primary, cyan accents, soft greens/reds for status states).
* **Smooth Animations:** Micro-transitions on hover, loading spinners, and CSS transition lines.

### Visual Sections
1. **Consumer Lag Explorer:** Displays active consumer groups, their subscribed topics, partition mappings, current offsets, end offsets, and calculated lag.
2. **Saga Lifecycle Tracker:** A searchable view of Saga instances. Selecting a Saga displays an interactive timeline of state transitions and compensating actions.
3. **Live Message Streamer:** A real-time console showing a formatted JSON viewer of payloads as they flow through the consumer pipeline, with search/filter capabilities.

---

## 3. Middleware Integration & Endpoints

### Registration
```csharp
app.UseqKafkaDashboard(options =>
{
    options.Path = "/qkafka";
    options.AllowOnlyLocalRequests = true; // Security setting
});
```

### Dashboard API Contracts
The middleware handles static files embedded as resource streams and exposes standard JSON endpoints:

* `GET /qkafka/api/overview` - Aggregate lag numbers and status.
* `GET /qkafka/api/consumers` - List of consumer groups, topics, partitions, and lag.
* `GET /qkafka/api/sagas` - Search and list Saga instances.
* `GET /qkafka/api/sagas/{correlationId}` - Complete history and state payload for a specific Saga.
* `GET /qkafka/api/stream` - Server-Sent Events (SSE) stream of real-time consume events.

---

## 4. Testing Matrix (3N Mandatory Coverage)

### N1 — Positive:
* Request the dashboard home path: Verify index.html, index.css, and JS files are served correctly.
* Connect to `/api/stream`: Assert that messages consumed in the application are pushed to the stream within milliseconds.

### N2 — Negative:
* Database or broker connection fails: Verify API endpoints return partial success responses with error banners rather than throwing unhandled exceptions.
* Multi-instance deploy (horizontal scaling): Dashboard endpoints remain consistent using query replicas or handling cluster environments.

### N3 — Invalid Input:
* Request `/api/sagas/<invalid-guid>`: Return `400 Bad Request` with structured error messages.
* Access dashboard path without authorization: Return `401 Unauthorized` or `403 Forbidden` according to security policies.
