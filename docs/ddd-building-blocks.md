# Domain Building Blocks — New BC Reference Guide

This guide covers the domain building blocks used in `issuance` and `entitymgmt`: aggregates, entities, value objects, and processes. Read it top-to-bottom once; keep the tables as a cheat sheet during code review.

---

## Quick Reference

| | **Aggregate** | **Entity** | **Value Object** | **Process** |
|---|---|---|---|---|
| **ID in domain** | ✅ `base.Entity` | ✅ `base.Entity` | ❌ none | ✅ `base.Process` → `base.Entity` |
| **ID in persistence** | ✅ `baserepo.Model` | ✅ `baserepo.Model` | ✅ `uuid.UUID` PK (GORM only) | ✅ `ProcessId uuid.UUID` (PK + FK) |
| **Has Repository** | ✅ yes | ❌ no | ❌ no | ✅ yes |
| **Own package** | ✅ yes | ✅ yes (inside aggregate) | ❌ same package as aggregate | ✅ yes |
| **Own mapper file** | ✅ yes | ✅ yes | ❌ inline in aggregate mapper | ✅ yes |
| **`BeforeSave`** | ✅ yes | only if owns VOs | ❌ no | only if owns a VO slice |
| **Cross-reference** | UUID only | n/a | n/a | UUID only |
| **DB tables** | 1 | n/a | n/a | 2 (`base_processes` + specific) |

---

## 1 — Package and File Naming

### Domain layer

```
internal/domain/asset/
├── asset.go                       ← aggregate root struct
├── asset_custom_data.go           ← value object (same package)
├── asset_capability_config.go     ← value object (same package)
├── asset_repository.go            ← Repository interface
├── asset_status.go                ← enums / status
├── asset_events.go                ← domain events
└── assetdocument/
│   └── asset_document.go          ← entity (own subpackage)
└── assetnetwork/
    └── asset_network.go           ← entity (own subpackage)
```

**Rules:**
- Aggregate package: `package asset` — singular, no suffix.
- Entity package: `package assetdocument` — **aggregate name + entity name**, no suffix. This is mandatory: it makes the GORM table name (`asset_documents`) match automatically, with no need for a `TableName()` override.
- Value object: lives in the aggregate package. No subpackage.

### Infra/repository layer

```
internal/infra/repository/asset/        ← package assetrepo
├── asset_persistence_model.go           ← aggregate model + BeforeSave
├── asset_mapper.go                      ← aggregate mapper (includes VO mapping)
├── asset_postgres_repository.go         ← Postgres implementation
├── asset_commonrepo_gen.go              ← generated CRUD (do not edit)
├── asset_capability_config_persistence_model.go ← value object model (same package)
├── asset_custom_data_persistence_model.go       ← value object model (same package)
├── assetdocument/                       ← package assetdocument (no repo suffix)
│   ├── asset_document_persistence_model.go
│   └── asset_document_mapper.go
└── assetnetwork/
    ├── asset_network_persistence_model.go
    └── asset_network_mapper.go
```

**Rules:**
- Aggregate repo package: `package assetrepo` — adds `repo` suffix.
- Entity infra package: `package assetdocument` — same name as domain, **no `repo` suffix**.
- Value object models: stay in the aggregate repo package, not in a subpackage.
- Every GORM model file ends in `_persistence_model.go` (aggregate, entity and value object), one struct per file, prefixed with the aggregate name.

---

## 2 — Domain Structs

### Aggregate and Entity — embed `base.Entity`

```go
// Aggregate root
type Asset struct {
    base.Entity                    // ID, CreatedAt, UpdatedAt
    AssetClassId uuid.UUID         // cross-aggregate: UUID only, never embed
    Status       Status
    Documents    []assetdocument.AssetDocument
    Networks     []assetnetwork.AssetNetwork
    CustomData   []CustomDataEntry // value objects: slice, no ID
}

// Entity
type AssetDocument struct {
    base.Entity                    // ID, CreatedAt, UpdatedAt
    FileName string
    Uri      string
}
```

### Value Object — no ID, no `base.Entity`

```go
type CustomDataEntry struct {
    Key   string
    Value string
}

type CapabilityConfig struct {
    CapabilityId   string
    Version        string
    InstanceParams map[string]any
}
```

### Cross-aggregate reference

Always a bare `uuid.UUID`. **Never** embed the full aggregate struct:

```go
// ✅ correct
AssetClassId uuid.UUID

// ❌ wrong
AssetClass assetclass.AssetClass
```

---

## 3 — Persistence Models

### Aggregate and Entity — embed `baserepo.Model`

```go
// Aggregate root (assetrepo package)
type Asset struct {
    baserepo.Model                 // Id, CreatedAt, UpdatedAt, DeletedAt
    AssetClassId uuid.UUID         `gorm:"type:uuid;not null;index"`
    Status       string            `gorm:"type:varchar(50);not null"`
    Documents    []assetdocument.AssetDocument  `gorm:"foreignKey:AssetId"`
    CustomData   []CapabilityConfig             `gorm:"foreignKey:AssetId"`
}

// Entity (assetdocument package)
type AssetDocument struct {
    baserepo.Model
    AssetId  uuid.UUID `gorm:"type:uuid;not null;index"` // explicit FK
    FileName string    `gorm:"type:varchar(255);not null"`
}
```

### Value Object — manual `uuid.UUID` PK, no `baserepo.Model`

```go
// Value object (assetrepo package, same as aggregate)
type CapabilityConfig struct {
    Id           uuid.UUID `gorm:"type:uuid;primarykey"`
    AssetId      uuid.UUID `gorm:"type:uuid;not null;index"` // FK to parent
    CapabilityId string    `gorm:"type:varchar(255);not null"`
    Version      string    `gorm:"type:varchar(100);not null"`
}
```

The value object ID **has no meaning in domain** — it exists only so GORM can manage rows. It is generated fresh on every save (see BeforeSave below).

### Table naming

GORM derives the table name from the struct name by default (plural snake\_case). Rely on this convention:

| Struct | Table |
|---|---|
| `Asset` | `assets` |
| `AssetDocument` | `asset_documents` |
| `CapabilityConfig` | `capability_configs` |

Only add `TableName()` when the default result is wrong. If you find yourself adding it for an entity, check the package name first — it probably doesn't include the aggregate prefix.

---

## 4 — BeforeSave: Nuke-and-Replace

Value objects have no stable identity, so GORM cannot update them in place. The pattern is: delete all existing children, then let GORM re-insert the new slice via `FullSaveAssociations`.

```go
// On the aggregate persistence model
func (a *Asset) BeforeSave(tx *gorm.DB) error {
    if err := tx.Where("asset_id = ?", a.Id).Delete(&CapabilityConfig{}).Error; err != nil {
        return err
    }
    if err := tx.Where("asset_id = ?", a.Id).Delete(&CustomDataEntry{}).Error; err != nil {
        return err
    }
    return nil
}
```

**Rules:**
- `BeforeSave` lives on the aggregate root persistence model.
- It deletes **value objects only** — not entities (entities have stable IDs and are managed explicitly).
- If an entity itself owns value objects, that entity also gets its own `BeforeSave`.
- Never put `BeforeSave` on a value object model.

---

## 4.1 — Entities: Stable Identity, Explicit Deletion

Entities are NOT nuked. Their `FromDomain` preserves `domain.Id` (see the entity
mapper in §5), so on the aggregate's `Save` — which runs with
`FullSaveAssociations: true` — GORM matches each child by primary key and issues
an **UPDATE**, not an INSERT. No duplication, no `BeforeSave` needed.

There is one catch that stable identity does NOT solve: **orphans**.
`FullSaveAssociations` upserts the children present in the in-memory slice, but it
does **not delete** the rows that you removed from the collection. A value object's
`BeforeSave` nuke covered this for free; an entity has no nuke, so a child you drop
from the aggregate stays in the database as a stale row.

Removal of an entity from a collection must therefore be **explicit**, via a
dedicated repository method that deletes it by id:

```go
// asset_postgres_repository.go
func (r *Postgres) DeleteDocument(ctx context.Context, id uuid.UUID) error {
    return r.getDB(ctx).Delete(&assetdocument.AssetDocument{}, id).Error
}

func (r *Postgres) DeleteNetwork(ctx context.Context, id uuid.UUID) error {
    return r.getDB(ctx).Delete(&assetnetwork.AssetNetwork{}, id).Error
}
```

**Rules:**
- Never delete-and-reinsert an entity to "refresh" it. That is value-object
  behaviour: it throws away the entity's identity, its `CreatedAt`/audit trail, and
  breaks any FK that points to it. If you only change a field, the aggregate `Save`
  already UPDATEs it in place — do nothing extra.
- Adding an entity to a collection needs no special handling: the aggregate `Save`
  inserts the new child (its id is preserved from the domain).
- Removing an entity from a collection is the ONLY case that needs explicit work:
  call the dedicated `Delete…(ctx, id)` repository method. The aggregate `Save`
  will not orphan-collect for you.
- A blanket nuke over an entity collection is wrong — it is surgical deletion by
  id, never a "delete all children then re-insert".

### Do NOT move orphan detection into a GORM hook

It is tempting to diff the incoming slice against the DB inside `BeforeUpdate` /
`AfterUpdate` and delete what's missing, so callers can't forget. Don't.

"Absent from the slice" is ambiguous: it means either *"the domain removed this
child"* or *"the caller didn't preload the collection"*. A hook cannot tell them
apart. GORM refuses to orphan-collect for exactly this reason. If any code path
saves the aggregate without preloading the collection, the hook reads an empty
slice and deletes **every** child — a silent mass delete triggered by an innocent
field update.

The failure modes are asymmetric, and that settles it:
- Forget the explicit delete → an **orphan row** stays. Visible in queries,
  detectable, recoverable.
- Hook diff + forgot to preload → **silent data loss**. Catastrophic.

Rule: **delete by id (declares intent), never by absence (infers intent).** Orphan
removal lives in the use case (aggregate `Save` + `Delete…(ctx, id)` in one
transaction), or behind a repo method that takes the removed ids **explicitly** —
never inferred from a slice, never in a lifecycle hook.

---

## 5 — Mappers

### Aggregate mapper

Lives in the aggregate repo package (`asset_mapper.go`). Maps the full graph:

```
ToDomain(model Asset) asset.Asset
FromDomain(domain asset.Asset) Asset
```

Internally calls entity mappers and maps value objects inline:

```go
func FromDomain(domain asset.Asset) Asset {
    return Asset{
        // ...
        Documents:  assetdocument.FromDomainList(domain.Documents, domain.Id),
        CustomData: fromDomainCustomData(domain.CustomData, domain.Id),
    }
}

// Value object mapping is inline — no separate mapper file
func fromDomainCustomData(vos []asset.CustomDataEntry, assetId uuid.UUID) []CustomDataEntry {
    result := make([]CustomDataEntry, 0, len(vos))
    for _, vo := range vos {
        result = append(result, CustomDataEntry{
            Id:      uuid.New(), // always a fresh UUID — VO has no stable identity
            AssetId: assetId,
            Key:     vo.Key,
            Value:   vo.Value,
        })
    }
    return result
}
```

### Entity mapper

Lives in the entity infra subpackage (`assetdocument/asset_document_mapper.go`). Receives the parent ID as a parameter:

```go
func FromDomain(domain assetdocument.AssetDocument, assetId uuid.UUID) AssetDocument
func FromDomainList(domains []assetdocument.AssetDocument, assetId uuid.UUID) []AssetDocument
func ToDomain(model AssetDocument) assetdocument.AssetDocument
```

**Rules:**
- Value objects → inline in the aggregate mapper, `uuid.New()` on every `FromDomain`.
- Entities → separate mapper file in the entity package, called from the aggregate mapper.
- `FromDomain` for both always receives the parent ID as an explicit parameter.

---

## Checklist — New Aggregate or Entity

Before opening a PR, verify:

- [ ] Aggregate package is `package <name>`, repo package is `package <name>repo`
- [ ] Entity package is `package <aggregatename><entityname>` — no `repo` suffix
- [ ] Value objects live in the aggregate package, not a subpackage
- [ ] Aggregate has `_commonrepo_gen.go`, `_postgres_repository.go`, `_mapper.go`, `_persistence_model.go`
- [ ] Entity has `_persistence_model.go` and `_mapper.go` in its own subpackage, no generated repo
- [ ] `BeforeSave` deletes value objects by parent FK before re-insert
- [ ] Value object `FromDomain` calls `uuid.New()` for the persistence ID
- [ ] Entity removal from a collection has an explicit `Delete…(ctx, id)` repository method — the aggregate `Save` does not orphan-collect
- [ ] Cross-aggregate references use `uuid.UUID` only — no embedded structs
- [ ] No `TableName()` override unless the GORM default is genuinely wrong

---

## 6 — Processes

A process models a long-running workflow that coordinates a domain operation across multiple steps (start → ordered → finished/failed). It is NOT a regular aggregate: it has its own base type, a split persistence strategy, and a strict state machine.

### What `base.Process` provides

```go
// iob-go-core/v4 — base.Process
type Process struct {
    Entity                                  // Id, CreatedAt, UpdatedAt
    Status              ProcessStatus        // STARTED | ORDERED | FINISHED | FAILED | ...
    Type                Type                 // process type constant (e.g. "FINALIZE_ASSET_CLASS")
    SigningType          SigningType          // CUSTODIAL | NON_CUSTODIAL
    EntityId            uuid.UUID            // the domain object being processed (asset, assetClass…)
    TransactionHash     string
    SignerDltAccountId  string
    Callback            callback.Callback
    UnsignedTransactionProcess *UnsignedTransactionProcess
}
```

`Id` is promoted from `base.Entity` through the embedding chain. **Never declare `ProcessId uuid.UUID` on the domain struct** — it is redundant and will cause a compile error.

### Domain struct

```go
// internal/domain/process/finalizeassetclass/finalize_asset_class_process.go
// package finalizeassetclassprocess

type FinalizeAssetClassProcess struct {
    base.Process                // ← Id promoted from here
    AssetClassId uuid.UUID      // domain-specific fields only
}
```

Only add fields that are not already covered by `base.Process`. The target entity reference goes in **both** `base.Process.EntityId` (generic, required by the framework) and a typed field like `AssetClassId` (for domain readability).

### Constructor pattern

The `New()` function creates the process in `Started` state. Always set `Id`, `Type`, `Status`, and `EntityId` on the embedded `base.Process`:

```go
func New(processId uuid.UUID, assetClassId uuid.UUID, signerDltAccountId string,
    configs ...Configurator) (*FinalizeAssetClassProcess, error) {

    newBase := base.NewProcess()
    newBase.Id = processId
    newBase.Type = base.Type(issuanceprocess.FinalizeAssetClass)
    newBase.Status = base.Started
    newBase.EntityId = assetClassId
    newBase.SignerDltAccountId = signerDltAccountId

    proc := &FinalizeAssetClassProcess{
        Process:      newBase,
        AssetClassId: assetClassId,
    }
    for _, c := range configs {
        c(proc)
    }
    return proc, validate(proc)
}
```

> **Error return type.** `New()` (and process services, command handlers, and `validate`) return a plain `error`, **not** a typed `*domainerrors.IssuanceError`. The bounded context migrated from typed domain errors to sentinel-style errors; use `error` everywhere. Domain error constructors (`domainerrors.New…DomainError(...)`) return values that satisfy `error`.

### Package naming

| Layer | Directory | Package |
|---|---|---|
| Domain | `internal/domain/process/{name}/` | `package {name}process` |
| Domain service | `internal/domain/process/{name}/service/` | `package service{name}process` |
| Infra repo | `internal/infra/repository/process/{name}/` | `package {name}processrepo` |
| App command | `internal/app/command/process/{name}/{action}/` | `package {action}{name}` |
| App service | `internal/app/service/process/{name}/{action}/` | `package {action}{name}` |

Examples: `finalizeassetclassprocess`, `finalizeassetclassprocessrepo`, `startfinalizeassetclass`, `orderfinalizeassetclass`.

### Split-table persistence

Every process type uses **two tables**:

| Table | Content |
|---|---|
| `base_processes` | Status, Type, TransactionHash, SignerDltAccountId, EntityId, Callback… |
| `{name}_processes` | `process_id` (PK + FK) + domain-specific columns |

The persistence model reflects this split:

```go
// internal/infra/repository/process/finalizeassetclass/
// package finalizeassetclassprocessrepo

type FinalizeAssetClassProcess struct {
    Process      baserepo.Process `gorm:"foreignKey:ProcessId;references:Id"` // → base_processes
    ProcessId    uuid.UUID        `gorm:"type:uuid;primaryKey"`               // PK and FK
    AssetClassId uuid.UUID        `gorm:"type:uuid;not null"`
}
```

`ProcessId` is the primary key of the type-specific table AND the foreign key to `base_processes.id`. GORM handles both rows in a single `Create` / `Save` call.

### Mapper — the critical rule

`FromDomain` must populate `ProcessId` explicitly from `entity.Id`:

```go
func FromDomain(domain finalizeassetclassprocess.FinalizeAssetClassProcess) FinalizeAssetClassProcess {
    return FinalizeAssetClassProcess{
        Process:      baserepo.FromDomain(domain.Process), // maps base_processes row
        ProcessId:    domain.Id,                           // ← must be set explicitly
        AssetClassId: domain.AssetClassId,
    }
}

func ToDomain(model FinalizeAssetClassProcess) finalizeassetclassprocess.FinalizeAssetClassProcess {
    return finalizeassetclassprocess.FinalizeAssetClassProcess{
        Process:      baserepo.ToDomain(model.Process),
        AssetClassId: model.AssetClassId,
    }
}
```

Do NOT set `ProcessId` in `ToDomain` — the `Id` comes from the embedded `baserepo.Process` via `baserepo.ToDomain`.

### SQL queries that JOIN base_processes

When writing custom queries, join on `base_processes` (the actual table name) and reference that alias in the WHERE clause. Using the wrong alias is a silent SQL error:

```go
// ✅ correct
r.getDB(ctx).
    Joins("JOIN base_processes ON base_processes.id = finalize_asset_class_processes.process_id").
    Where("finalize_asset_class_processes.asset_class_id = ? AND base_processes.status NOT IN (?)", id, statuses)

// ❌ wrong — "processes" table does not exist
    Where("... AND processes.status NOT IN (?)", ...)
```

### Events

Every process declares a standard set of events in `{name}_process_events.go`. All event structs embed `event.BaseEvent` and carry `ProcessId uuid.UUID` — this is a field on the event struct, NOT the process struct:

```go
// internal/domain/process/finalizeassetclass/finalize_asset_class_process_events.go

type StartedEvent struct {
    event.BaseEvent
    ProcessId uuid.UUID   // ← field on the event, correct
}

type OrderedEvent struct {
    event.BaseEvent
    ProcessId       uuid.UUID
    TransactionHash string
}

type TransitToOrderedEvent struct {
    event.BaseEvent
    ProcessId uuid.UUID
}

type TransitToFinishedEvent struct {
    event.BaseEvent
    ProcessId uuid.UUID
}

type TransitToFailedEvent struct {
    event.BaseEvent
    ProcessId uuid.UUID
}
```

Events get `ProcessId` from `proc.Id` (promoted field), never from a `ProcessId` field on the process struct.

### Composite literal pitfall

`base.Process` embeds `base.Entity`. Go does not allow using promoted field names in composite literals — you must navigate the full embedding path:

```go
// ✅ correct — explicit nesting
proc := &FinalizeAssetClassProcess{
    Process: base.Process{
        Entity: base.Entity{Id: someId},
        Status: base.Started,
    },
    AssetClassId: assetClassId,
}

// ✅ also correct — variable + field assignment
proc := &FinalizeAssetClassProcess{AssetClassId: assetClassId}
proc.Id = someId       // promoted field, assignable outside a literal
proc.Status = base.Started

// ❌ compile error — "unknown field ProcessId in struct literal"
proc := &FinalizeAssetClassProcess{
    ProcessId: someId, // does not exist on the domain struct
}
```

In tests, prefer the variable form when you don't have a `base` import in scope.

### Test factory

Process test factories live in the domain package with the `//go:build test || integration` tag:

```go
//go:build test || integration

func (f *TestFactory) CreateEntity(configs ...Configurator) *FinalizeAssetClassProcess {
    uniqueId := uuid.New()
    proc := &FinalizeAssetClassProcess{
        Process: base.Process{
            Entity:   base.Entity{Id: uniqueId},
            Status:   base.Started,
            Type:     base.Type(issuanceprocess.FinalizeAssetClass),
            EntityId: uuid.New(), // must not be Nil
        },
        AssetClassId: uuid.New(),
    }
    for _, c := range configs {
        c(proc)
    }
    return proc
}
```

Always set `EntityId` to a non-nil UUID. Some event check functions validate `ProcessId != uuid.Nil`, so the factory must also ensure `Id` is set (it is, via `base.Entity{Id: uniqueId}`).

---

## Checklist — New Process

Before opening a PR, verify:

- [ ] Domain struct embeds `base.Process` only — no `ProcessId uuid.UUID` field
- [ ] `New()` sets `Id`, `Type`, `Status`, `EntityId`, and `SignerDltAccountId` on the embedded `Process`
- [ ] Process type constant declared in `internal/domain/process/issuance_process.go`
- [ ] Persistence model has `ProcessId uuid.UUID` as PK + FK and `Process baserepo.Process` as GORM relation
- [ ] Mapper `FromDomain` sets `ProcessId: domain.Id` explicitly
- [ ] Custom SQL queries reference `base_processes.{column}`, not `processes.{column}`
- [ ] Events carry `ProcessId uuid.UUID` as a field; populated from `proc.Id`
- [ ] Test factory builds the full nesting: `base.Process{Entity: base.Entity{Id: x}, EntityId: nonNilUUID, ...}`
- [ ] Package names follow the convention: domain = `{name}process`, repo = `{name}processrepo`
