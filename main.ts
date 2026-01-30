import {
  App,
  Modal,
  Notice,
  Plugin,
  PluginSettingTab,
  Setting,
  TFile,
  TFolder,
  normalizePath,
  stringifyYaml,
  parseYaml,
} from "obsidian";

type TypeName = string;

interface TypeSchema {
  keysOrdered: string[]; // includes "Type"
}

export interface TypeSyncSettings {
  mySetting: string;
  syncOrder: boolean;
  debounceMs: number;
}

const DEFAULT_SETTINGS: TypeSyncSettings = {
  mySetting: "default",
  syncOrder: true,
  debounceMs: 350,
};

const TYPE_KEY = "Type";

interface Snapshot {
  typeValue: TypeName | null;
  keysSet: Set<string>;          // from parsed object keys
  keysOrdered: string[];         // from frontmatter text order (fallback to keysSet if no FM)
  frontmatterObj: Record<string, any>;
  hasFrontmatter: boolean;
  rawContent: string;            // for minimal revert when needed
}

type Diff = {
  added: string[];
  removed: string[];
  orderChanged: boolean;
};

type BulkRunOptions = {
  title: string;
  files: TFile[];
  perFile: (file: TFile, index: number, total: number) => Promise<void>;
  onDone?: (completed: number, total: number, canceled: boolean, failures: number) => void;
};

class BulkProgressModal extends Modal {
  private canceled = false;
  private total = 0;
  private completed = 0;
  private failures = 0;
  private titleText = "TypeSync";
  private statusText = "";
  private progressEl!: HTMLProgressElement;
  private statusEl!: HTMLElement;
  private countsEl!: HTMLElement;
  private cancelBtn!: HTMLButtonElement;

  constructor(app: App) {
    super(app);
  }

  setTitleText(t: string) {
    this.titleText = t;
    if (this.titleEl) this.titleEl.setText(t);
  }

  setTotal(total: number) {
    this.total = total;
    this.refresh();
  }

  setStatus(text: string) {
    this.statusText = text;
    this.refresh();
  }

  incrementCompleted() {
    this.completed += 1;
    this.refresh();
  }

  incrementFailures() {
    this.failures += 1;
    this.refresh();
  }

  getCanceled() {
    return this.canceled;
  }

  getFailures() {
    return this.failures;
  }

  getCompleted() {
    return this.completed;
  }

  onOpen() {
    const { contentEl } = this;
    contentEl.empty();

    this.titleEl.setText(this.titleText);

    const wrap = contentEl.createDiv({ cls: "typesync-progress" });

    this.statusEl = wrap.createEl("div", { text: this.statusText || "" });

    this.progressEl = wrap.createEl("progress", { cls: "typesync-progress-bar" });
    this.progressEl.max = Math.max(1, this.total);
    this.progressEl.value = this.completed;

    const row = wrap.createDiv({ cls: "typesync-progress-row" });
    this.countsEl = row.createEl("div", { text: "" });

    this.cancelBtn = row.createEl("button", { text: "Cancel" });
    this.cancelBtn.addEventListener("click", () => {
      this.canceled = true;
      this.cancelBtn.disabled = true;
      this.setStatus("Canceling… finishing current file.");
    });

    this.refresh();
  }

  private refresh() {
    if (this.progressEl) {
      this.progressEl.max = Math.max(1, this.total);
      this.progressEl.value = this.completed;
    }
    if (this.statusEl) this.statusEl.setText(this.statusText || "");
    if (this.countsEl) {
      const t = Math.max(0, this.total);
      this.countsEl.setText(
        `Progress: ${this.completed}/${t}${this.failures ? ` • Failures: ${this.failures}` : ""}`
      );
    }
  }
}

type ChangeDecision =
  | { kind: "apply" }
  | { kind: "revert" };

class ConfirmSchemaChangeModal extends Modal {
  private resolved = false;
  private decision: ChangeDecision = { kind: "revert" };

  constructor(
    app: App,
    private typeValue: string,
    private added: string[],
    private removed: string[],
    private onResolve: (d: ChangeDecision) => void
  ) {
    super(app);
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.empty();

    this.titleEl.setText("TypeSync");

    const intro = contentEl.createEl("p");
    intro.setText(`Update schema for Type "${this.typeValue}"?`);

    if (this.added.length > 0) {
      contentEl.createEl("div", { text: "Added:" });
      const ul = contentEl.createEl("ul", { cls: "typesync-list" });
      this.added.forEach((k) => ul.createEl("li", { text: k }));
    }

    if (this.removed.length > 0) {
      contentEl.createEl("div", { text: "Removed:" });
      const ul = contentEl.createEl("ul", { cls: "typesync-list" });
      this.removed.forEach((k) => ul.createEl("li", { text: k }));
    }

    const buttons = contentEl.createDiv({ cls: "typesync-progress-row" });

    const applyBtn = buttons.createEl("button", { text: "Apply to all files of this Type" });
    applyBtn.classList.add("mod-cta");
    applyBtn.addEventListener("click", () => {
      this.resolved = true;
      this.decision = { kind: "apply" };
      this.onResolve(this.decision);
      this.close();
    });

    const revertBtn = buttons.createEl("button", { text: "Revert this note" });
    revertBtn.addEventListener("click", () => {
      this.resolved = true;
      this.decision = { kind: "revert" };
      this.onResolve(this.decision);
      this.close();
    });
  }

  onClose(): void {
    if (!this.resolved) {
      this.onResolve({ kind: "revert" });
    }
    this.contentEl.empty();
  }
}

type TypeChangeDecision =
  | { kind: "changeToExisting"; targetType: string }      // destructive align to existing schema
  | { kind: "createNewType"; newType: string }            // schema from current file
  | { kind: "renameType"; fromType: string; toType: string }
  | { kind: "cancel" };

class TypeChangeModal extends Modal {
  private resolved = false;
  private decision: TypeChangeDecision = { kind: "cancel" };

  constructor(
    app: App,
    private prevType: string | null,
    private nextType: string,
    private nextTypeExists: boolean,
    private onResolve: (d: TypeChangeDecision) => void
  ) {
    super(app);
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.empty();
    this.titleEl.setText("TypeSync");

    if (this.nextTypeExists) {
      contentEl.createEl("p", {
        text:
          `Type "${this.nextType}" already exists.\n` +
          `Do you want to change this note to Type "${this.nextType}" and align its properties to that schema?` +
          `\nWarning: This removes properties not in the schema.`,
      });

      const row = contentEl.createDiv({ cls: "typesync-progress-row" });
      const yesBtn = row.createEl("button", { text: "Change type + align" });
      yesBtn.classList.add("mod-cta");
      yesBtn.addEventListener("click", () => {
        this.resolved = true;
        this.decision = { kind: "changeToExisting", targetType: this.nextType };
        this.onResolve(this.decision);
        this.close();
      });

      const cancelBtn = row.createEl("button", { text: "Cancel" });
      cancelBtn.addEventListener("click", () => {
        this.resolved = true;
        this.decision = { kind: "cancel" };
        this.onResolve(this.decision);
        this.close();
      });
      return;
    }

    // next type does not exist as schema
    if (this.prevType) {
      contentEl.createEl("p", {
        text:
          `Type "${this.nextType}" does not exist yet.\n` +
          `Do you want to create a new type "${this.nextType}", or rename the existing type "${this.prevType}" to "${this.nextType}"?`,
      });

      const row = contentEl.createDiv({ cls: "typesync-progress-row" });

      const createBtn = row.createEl("button", { text: "Create new type" });
      createBtn.classList.add("mod-cta");
      createBtn.addEventListener("click", () => {
        this.resolved = true;
        this.decision = { kind: "createNewType", newType: this.nextType };
        this.onResolve(this.decision);
        this.close();
      });

      const renameBtn = row.createEl("button", { text: `Rename "${this.prevType}" → "${this.nextType}"` });
      renameBtn.addEventListener("click", () => {
        this.resolved = true;
        this.decision = { kind: "renameType", fromType: this.prevType!, toType: this.nextType };
        this.onResolve(this.decision);
        this.close();
      });

      const cancelBtn = row.createEl("button", { text: "Cancel" });
      cancelBtn.addEventListener("click", () => {
        this.resolved = true;
        this.decision = { kind: "cancel" };
        this.onResolve(this.decision);
        this.close();
      });
      return;
    }

    // assigning type for first time and schema doesn't exist
    contentEl.createEl("p", {
      text:
        `Type "${this.nextType}" does not exist yet.\n` +
        `TypeSync will create a new schema for "${this.nextType}" from this note's current properties.`,
    });

    const row = contentEl.createDiv({ cls: "typesync-progress-row" });

    const okBtn = row.createEl("button", { text: "OK" });
    okBtn.classList.add("mod-cta");
    okBtn.addEventListener("click", () => {
      this.resolved = true;
      this.decision = { kind: "createNewType", newType: this.nextType };
      this.onResolve(this.decision);
      this.close();
    });

    const cancelBtn = row.createEl("button", { text: "Cancel" });
    cancelBtn.addEventListener("click", () => {
      this.resolved = true;
      this.decision = { kind: "cancel" };
      this.onResolve(this.decision);
      this.close();
    });
  }

  onClose(): void {
    if (!this.resolved) this.onResolve({ kind: "cancel" });
    this.contentEl.empty();
  }
}

class TypeSyncSettingTab extends PluginSettingTab {
  constructor(app: App, private plugin: TypeSyncPlugin) {
    super(app, plugin);
  }

  display(): void {
    const { containerEl } = this;
    containerEl.empty();

    containerEl.createEl("h2", { text: "TypeSync" });

    new Setting(containerEl)
      .setName("Sync property order")
      .setDesc("If enabled, reordering properties in one note will silently propagate to the schema and all notes of that Type.")
      .addToggle((toggle) =>
        toggle
          .setValue(this.plugin.settings.syncOrder)
          .onChange(async (v) => {
            this.plugin.settings.syncOrder = v;
            await this.plugin.savePluginData();
          })
      );

    new Setting(containerEl)
      .setName("Debounce (ms)")
      .setDesc("Delay before TypeSync reacts to edits. Higher values reduce jitter with the Properties UI.")
      .addSlider((s) =>
        s
          .setLimits(0, 1500, 50)
          .setValue(this.plugin.settings.debounceMs)
          .setDynamicTooltip()
          .onChange(async (v) => {
            this.plugin.settings.debounceMs = v;
            await this.plugin.savePluginData();
          })
      );
  }
}

export default class TypeSyncPlugin extends Plugin {
  settings: TypeSyncSettings;

  private schemas: Record<TypeName, TypeSchema> = {};
  private snapshots: Map<string, Snapshot> = new Map();

  // suppression so TypeSync edits never cause prompts
  private suppressedPaths: Map<string, number> = new Map();

  // debounce modify events per file
  private debounceTimers: Map<string, number> = new Map();

  // lock schema while modal or bulk op
  private schemaLocked = false;

  async onload(): Promise<void> {
    const loaded = await this.loadData();
    this.settings = Object.assign({}, DEFAULT_SETTINGS, loaded?.settings ?? loaded ?? {});
    this.schemas = loaded?.schemas ?? loaded?.schemasByType ?? loaded?.schemas ?? {};

    // normalize schemas
    for (const [t, s] of Object.entries(this.schemas)) {
      if (!s || !Array.isArray((s as any).keysOrdered)) {
        this.schemas[t] = { keysOrdered: [TYPE_KEY] };
      } else if (!(s as any).keysOrdered.includes(TYPE_KEY)) {
        (s as any).keysOrdered.unshift(TYPE_KEY);
      }
    }

    this.addSettingTab(new TypeSyncSettingTab(this.app, this));

    this.addCommand({
      id: "typesync-sync-type-to-this-file",
      name: "TypeSync: Sync Type to This File (schema only)",
      callback: async () => {
        const file = this.app.workspace.getActiveFile();
        if (!file) { new Notice("No active file."); return; }
        if (file.extension !== "md") { new Notice("TypeSync only works on markdown files."); return; }

        const snap = await this.buildSnapshot(file);
        if (!snap.typeValue) { new Notice(`No "${TYPE_KEY}" set on this note.`); return; }
        const typeValue = snap.typeValue;

        this.schemas[typeValue] = { keysOrdered: [...snap.keysOrdered] };
        await this.savePluginData();
        new Notice(`TypeSync: schema for "${typeValue}" set to match this file (no edits made).`);
      },
    });

    this.addCommand({
      id: "typesync-reconcile-current-type",
      name: "TypeSync: Reconcile Current Type",
      callback: async () => {
        const file = this.app.workspace.getActiveFile();
        if (!file) { new Notice("No active file."); return; }
        if (file.extension !== "md") { new Notice("TypeSync only works on markdown files."); return; }

        const snap = await this.buildSnapshot(file);
        if (!snap.typeValue) { new Notice(`No "${TYPE_KEY}" set on this note.`); return; }
        const typeValue = snap.typeValue;
        const schema = this.schemas[typeValue];
        if (!schema) { new Notice(`No schema exists for Type "${typeValue}".`); return; }

        await this.runBulk({
          title: `Reconciling Type "${typeValue}"`,
          files: this.getFilesByType(typeValue),
          perFile: async (f) => {
            await this.rewriteToSchema(f, typeValue, { preserveOverlapValues: true });
          },
          onDone: (done, total, canceled, failures) => {
            new Notice(`TypeSync: Reconciled ${done}/${total}${canceled ? " (canceled)" : ""}${failures ? ` • failures: ${failures}` : ""}`);
          },
        });
      },
    });

    this.addCommand({
      id: "typesync-reconcile-all-types",
      name: "TypeSync: Reconcile All Types",
      callback: async () => {
        const allTypes = Object.keys(this.schemas);
        if (allTypes.length === 0) { new Notice("TypeSync: no schemas found."); return; }

        const files = this.app.vault.getMarkdownFiles().filter((f) => f.extension === "md");
        // We'll reconcile by scanning each file's Type and applying its schema
        await this.runBulk({
          title: "Reconciling all Types",
          files,
          perFile: async (f) => {
            const snap = await this.buildSnapshot(f);
            if (!snap.typeValue) return;
            if (!this.schemas[snap.typeValue]) return;
            await this.rewriteToSchema(f, snap.typeValue, { preserveOverlapValues: true });
          },
          onDone: (done, total, canceled, failures) => {
            new Notice(`TypeSync: Reconcile complete ${done}/${total}${canceled ? " (canceled)" : ""}${failures ? ` • failures: ${failures}` : ""}`);
          },
        });
      },
    });

    this.registerEvent(
      this.app.vault.on("modify", (file) => {
        if (!(file instanceof TFile)) return;
        if (file.extension !== "md") return;

        const path = file.path;
        if (this.isSuppressed(path)) return;

        // debounce
        const existing = this.debounceTimers.get(path);
        if (existing) window.clearTimeout(existing);

        const timer = window.setTimeout(() => {
          this.debounceTimers.delete(path);
          this.handleModify(file).catch((e) => console.error("TypeSync modify error", e));
        }, this.settings.debounceMs);

        this.debounceTimers.set(path, timer);
      })
    );

    // seed snapshots
    await this.seedSnapshots();
  }

  async savePluginData(): Promise<void> {
    await this.saveData({
      settings: this.settings,
      schemas: this.schemas,
    });
  }


  async saveSettings(): Promise<void> {
    await this.savePluginData();
  }

  private async seedSnapshots(): Promise<void> {
    const files = this.app.vault.getMarkdownFiles();
    for (const f of files) {
      try {
        const snap = await this.buildSnapshot(f);
        this.snapshots.set(f.path, snap);
      } catch {
        // ignore
      }
    }
  }

  private isSuppressed(path: string): boolean {
    return (this.suppressedPaths.get(path) ?? 0) > 0;
  }

  private suppressOnce(path: string): void {
    const c = (this.suppressedPaths.get(path) ?? 0) + 1;
    this.suppressedPaths.set(path, c);
    window.setTimeout(() => {
      const cur = this.suppressedPaths.get(path) ?? 0;
      if (cur <= 1) this.suppressedPaths.delete(path);
      else this.suppressedPaths.set(path, cur - 1);
    }, 1000);
  }

  private async handleModify(file: TFile): Promise<void> {
    if (this.schemaLocked) return;

    const prev = this.snapshots.get(file.path);
    const next = await this.buildSnapshot(file);

    // update snapshot early to avoid repeated prompts
    this.snapshots.set(file.path, next);

    // if Type removed: silently stop syncing this note
    if (prev?.typeValue && !next.typeValue) {
      return;
    }

    // type assignment/change logic
    if ((prev?.typeValue ?? null) !== (next.typeValue ?? null)) {
      await this.handleTypeValueChange(file, prev ?? null, next);
      return;
    }

    // if untyped: do nothing
    if (!next.typeValue) return;

    const typeValue = next.typeValue;

    // ensure schema exists
    if (!this.schemas[typeValue]) {
      // create schema from current file properties/order (includes Type)
      this.schemas[typeValue] = { keysOrdered: [...next.keysOrdered] };
      await this.savePluginData();
      return;
    }

    const schema = this.schemas[typeValue];
    const diff = this.computeDiff(prev ?? next, next);

    // order-only change: silently update schema order and propagate, if enabled
    if (this.settings.syncOrder && diff.added.length === 0 && diff.removed.length === 0 && diff.orderChanged) {
      // update schema to match this file order (but only if key set is exactly schema keys set)
      const schemaSet = new Set(schema.keysOrdered);
      const nextSet = new Set(next.keysSet);

      // enforce "no non-schema keys" – if user somehow introduced drift, reconcile
      const sameSet =
        schemaSet.size === nextSet.size &&
        [...schemaSet].every((k) => nextSet.has(k));

      if (!sameSet) {
        await this.rewriteToSchema(file, typeValue, { preserveOverlapValues: true });
        return;
      }

      schema.keysOrdered = [...next.keysOrdered];
      await this.savePluginData();

      await this.runBulk({
        title: `Syncing order for Type "${typeValue}"`,
        files: this.getFilesByType(typeValue).filter((f) => f.path !== file.path),
        perFile: async (f) => {
          await this.rewriteToSchema(f, typeValue, { preserveOverlapValues: true });
        },
      });
      return;
    }

    // schema key changes (add/remove): consolidated prompt
    if (diff.added.length > 0 || diff.removed.length > 0) {
      // If user added/removed Type itself: treat separately
      if (diff.removed.includes(TYPE_KEY)) return; // handled above as untype
      if (diff.added.includes(TYPE_KEY)) {
        // Type got added somehow but prev already typed; unusual. We'll reconcile.
        await this.rewriteToSchema(file, typeValue, { preserveOverlapValues: true });
        return;
      }

      // Only prompt if the file is typed and schema exists
      const decision = await this.promptSchemaChange(typeValue, diff.added, diff.removed);
      if (decision.kind === "apply") {
        // update schema:
        // - key set becomes next keysSet (must include Type)
        // - if order sync enabled, order becomes next.keysOrdered
        // - if order sync disabled, preserve existing schema order and append new keys at end, remove deleted keys
        const nextKeysSet = new Set(next.keysSet);
        nextKeysSet.add(TYPE_KEY);

        if (this.settings.syncOrder) {
          // schema order becomes next order, filtered to keysSet (safety)
          const filtered = next.keysOrdered.filter((k) => nextKeysSet.has(k));
          // ensure all keys are included (if parsing missed something)
          const missing = [...nextKeysSet].filter((k) => !filtered.includes(k));
          schema.keysOrdered = [...filtered, ...missing];
        } else {
          const cur = schema.keysOrdered.filter((k) => nextKeysSet.has(k));
          const missing = [...nextKeysSet].filter((k) => !cur.includes(k));
          schema.keysOrdered = [...cur, ...missing];
        }

        await this.savePluginData();

        // propagate across all files of that type
        await this.runBulk({
          title: `Updating Type "${typeValue}"`,
          files: this.getFilesByType(typeValue),
          perFile: async (f) => {
            await this.rewriteToSchema(f, typeValue, {
              preserveOverlapValues: true,
              // special: if removed+added looks like rename, preserve values from removed->added
              renameHint: (() => {
                const from = diff.removed.length === 1 ? diff.removed[0] : undefined;
                const to = diff.added.length === 1 ? diff.added[0] : undefined;
                return from && to ? { from, to } : undefined;
              })(),
            });
          },
        });
      } else {
        // revert this file to schema using prev snapshot values (best effort)
        if (prev) {
          await this.rewriteExactFrontmatterFromSnapshot(file, prev);
        } else {
          await this.rewriteToSchema(file, typeValue, { preserveOverlapValues: true });
        }
      }
    }
  }

  private computeDiff(prev: Snapshot, next: Snapshot): Diff {
    const prevSet = new Set(prev.keysSet);
    const nextSet = new Set(next.keysSet);

    const added = [...nextSet].filter((k) => !prevSet.has(k));
    const removed = [...prevSet].filter((k) => !nextSet.has(k));

    // order changed detection (only meaningful if same set)
    let orderChanged = false;
    if (prev.keysOrdered.length > 0 && next.keysOrdered.length > 0) {
      if (
        prev.keysOrdered.length === next.keysOrdered.length &&
        prev.keysOrdered.every((k, i) => next.keysOrdered[i] === k)
      ) {
        orderChanged = false;
      } else {
        // If they are permutations of same set, treat as order change
        const sameSet =
          prevSet.size === nextSet.size &&
          [...prevSet].every((k) => nextSet.has(k));
        orderChanged = sameSet;
      }
    }

    return { added, removed, orderChanged };
  }

  private async promptSchemaChange(typeValue: string, added: string[], removed: string[]): Promise<ChangeDecision> {
    this.schemaLocked = true;
    try {
      return await new Promise<ChangeDecision>((resolve) => {
        new ConfirmSchemaChangeModal(this.app, typeValue, added, removed, resolve).open();
      });
    } finally {
      this.schemaLocked = false;
    }
  }

  private async handleTypeValueChange(file: TFile, prev: Snapshot | null, next: Snapshot): Promise<void> {
    const prevType = prev?.typeValue ?? null;
    const nextType = next.typeValue ?? null;

    // Type removed (handled earlier)
    if (!nextType) return;

    const nextExists = !!this.schemas[nextType];

    // If prevType is null and nextType exists -> ask to align (destructive) or cancel (remove Type)
    if (!prevType && nextExists) {
      const ok = await this.promptYesNo(
        `Update this file to match Type "${nextType}"? Warning: This removes properties not in the schema.`,
        "Align to schema",
        "Cancel"
      );
      if (!ok) {
        // revert: remove Type, restore prior snapshot if possible
        if (prev) await this.rewriteExactFrontmatterFromSnapshot(file, prev);
        else await this.removeTypeKey(file);
        return;
      }

      await this.rewriteToSchema(file, nextType, { preserveOverlapValues: true });
      return;
    }

    // If prevType is null and nextType doesn't exist -> create schema from this file and keep
    if (!prevType && !nextExists) {
      // prompt via modal (OK / Cancel)
      const decision = await new Promise<TypeChangeDecision>((resolve) => {
        this.schemaLocked = true;
        new TypeChangeModal(this.app, null, nextType, false, resolve).open();
      }).finally(() => {
        this.schemaLocked = false;
      });

      if (decision.kind === "cancel") {
        if (prev) await this.rewriteExactFrontmatterFromSnapshot(file, prev);
        else await this.removeTypeKey(file);
        return;
      }

      // create new type from current file order/keys
      this.schemas[nextType] = { keysOrdered: [...next.keysOrdered] };
      await this.savePluginData();
      return;
    }

    // If prevType exists and nextType exists -> change this note to existing schema (destructive align)
    if (prevType && nextExists) {
      const decision = await new Promise<TypeChangeDecision>((resolve) => {
        this.schemaLocked = true;
        new TypeChangeModal(this.app, prevType, nextType, true, resolve).open();
      }).finally(() => {
        this.schemaLocked = false;
      });

      if (decision.kind !== "changeToExisting") {
        // revert Type back
        if (prev) await this.rewriteExactFrontmatterFromSnapshot(file, prev);
        else {
          await this.setTypeValue(file, prevType);
        }
        return;
      }

      // align to target schema, preserving overlap values
      await this.setTypeValue(file, nextType); // ensure Type is correct
      await this.rewriteToSchema(file, nextType, { preserveOverlapValues: true });
      return;
    }

    // If prevType exists and nextType does not exist -> create new type or rename current type
    if (prevType && !nextExists) {
      const decision = await new Promise<TypeChangeDecision>((resolve) => {
        this.schemaLocked = true;
        new TypeChangeModal(this.app, prevType, nextType, false, resolve).open();
      }).finally(() => {
        this.schemaLocked = false;
      });

      if (decision.kind === "cancel") {
        if (prev) await this.rewriteExactFrontmatterFromSnapshot(file, prev);
        else await this.setTypeValue(file, prevType);
        return;
      }

      if (decision.kind === "createNewType") {
        this.schemas[nextType] = { keysOrdered: [...next.keysOrdered] };
        await this.savePluginData();
        return;
      }

      if (decision.kind === "renameType") {
        // rename schema + update all notes of prevType to nextType
        const fromType = decision.fromType;
        const toType = decision.toType;

        const schema = this.schemas[fromType];
        if (!schema) {
          // fallback: create schema from current file
          this.schemas[toType] = { keysOrdered: [...next.keysOrdered] };
        } else {
          this.schemas[toType] = schema;
        }
        delete this.schemas[fromType];
        await this.savePluginData();

        const files = this.getFilesByType(fromType);

        await this.runBulk({
          title: `Renaming Type "${fromType}" → "${toType}"`,
          files,
          perFile: async (f) => {
            await this.setTypeValue(f, toType);
            await this.rewriteToSchema(f, toType, { preserveOverlapValues: true });
          },
          onDone: () => {
            new Notice(`TypeSync: renamed "${fromType}" → "${toType}".`);
          },
        });

        return;
      }
    }
  }

  private async promptYesNo(body: string, yesText = "Yes", noText = "No"): Promise<boolean> {
    this.schemaLocked = true;
    try {
      return await new Promise<boolean>((resolve) => {
        const m = new Modal(this.app);
        m.onOpen = () => {
          const { contentEl } = m;
          contentEl.empty();
          m.titleEl.setText("TypeSync");

          contentEl.createEl("p", { text: body });
          const row = contentEl.createDiv({ cls: "typesync-progress-row" });

          const yesBtn = row.createEl("button", { text: yesText });
          yesBtn.classList.add("mod-cta");
          yesBtn.addEventListener("click", () => {
            resolve(true);
            m.close();
          });

          const noBtn = row.createEl("button", { text: noText });
          noBtn.addEventListener("click", () => {
            resolve(false);
            m.close();
          });
        };
        m.onClose = () => {
          // dismiss = No
          // (only if not resolved yet; simplest: resolve(false) is okay but could double-resolve; ignore)
        };
        m.open();
      });
    } finally {
      this.schemaLocked = false;
    }
  }

  private getFilesByType(typeValue: string): TFile[] {
    return this.app.vault.getMarkdownFiles().filter((f) => {
      const snap = this.snapshots.get(f.path);
      if (snap) return snap.typeValue === typeValue;
      return false;
    });
  }

  private async runBulk(opts: BulkRunOptions): Promise<void> {
    const modal = new BulkProgressModal(this.app);
    modal.setTitleText(opts.title);
    modal.setTotal(opts.files.length);
    modal.setStatus("Starting…");

    this.schemaLocked = true;
    modal.open();

    let failures = 0;
    try {
      const total = opts.files.length;
      for (let i = 0; i < total; i++) {
        if (modal.getCanceled()) break;

        const f = opts.files[i];
        if (!f) continue;
        modal.setStatus(`Updating: ${f.path}`);
        try {
          await opts.perFile(f, i, total);
        } catch (e) {
          failures += 1;
          modal.incrementFailures();
          console.error("TypeSync bulk file error", f.path, e);
        }

        modal.incrementCompleted();

        // yield to UI every few files
        if (i % 15 === 0) {
          await new Promise((r) => window.setTimeout(r, 0));
        }
      }
    } finally {
      this.schemaLocked = false;
      modal.close();
      opts.onDone?.(modal.getCompleted(), opts.files.length, modal.getCanceled(), failures);
    }
  }

  private async buildSnapshot(file: TFile): Promise<Snapshot> {
    const rawContent = await this.app.vault.read(file);
    const { frontmatterText, bodyText, hasFrontmatter } = this.extractFrontmatter(rawContent);

    const fmObj: Record<string, any> = hasFrontmatter ? (parseYaml(frontmatterText) ?? {}) : {};
    const keysSet = new Set(Object.keys(fmObj));

    const typeValue = this.extractTypeValue(fmObj);

    const keysOrdered = hasFrontmatter
      ? this.extractOrderedKeysFromFrontmatterText(frontmatterText, keysSet)
      : [...keysSet];

    // if has Type but order parsing missed it, include it
    if (keysSet.has(TYPE_KEY) && !keysOrdered.includes(TYPE_KEY)) {
      keysOrdered.unshift(TYPE_KEY);
    }

    return {
      typeValue,
      keysSet,
      keysOrdered,
      frontmatterObj: fmObj,
      hasFrontmatter,
      rawContent,
    };
  }

  private extractTypeValue(fmObj: Record<string, any>): string | null {
    const v = fmObj?.[TYPE_KEY];
    if (typeof v !== "string") return null;
    const t = v.trim();
    return t.length ? t : null;
  }

  private extractFrontmatter(content: string): { frontmatterText: string; bodyText: string; hasFrontmatter: boolean } {
    // Standard YAML frontmatter: starts at first line with --- and ends at next --- line
    if (!content.startsWith("---\n") && content !== "---") {
      return { frontmatterText: "", bodyText: content, hasFrontmatter: false };
    }

    const end = content.indexOf("\n---", 4);
    if (end === -1) return { frontmatterText: "", bodyText: content, hasFrontmatter: false };

    const fmText = content.slice(4, end).replace(/\n$/, "");
    const after = content.slice(end + 4); // after "\n---"
    const body = after.startsWith("\n") ? after.slice(1) : after;

    return { frontmatterText: fmText, bodyText: body, hasFrontmatter: true };
  }

  private extractOrderedKeysFromFrontmatterText(frontmatterText: string, keysSet: Set<string>): string[] {
    const lines = frontmatterText.split("\n");
    const keys: string[] = [];

    // Very permissive top-level key matcher: "Key:" at column 0
    // We don't preserve comments/formatting, so this is good enough for ordering.
    const re = /^([^\s#][^:]*):\s*(.*)?$/;

    for (const line of lines) {
      const m = line.match(re);
      if (!m) continue;
      const keyRaw = m[1];
      if (!keyRaw) continue;
      const key = keyRaw.trim();
      if (key.length === 0) continue;
      if (!keysSet.has(key)) continue; // ignore things parseYaml didn't treat as key
      if (!keys.includes(key)) keys.push(key);
    }

    // ensure all keys are present (in case parsing missed)
    const missing = [...keysSet].filter((k) => !keys.includes(k));
    return [...keys, ...missing];
  }

  private async rewriteExactFrontmatterFromSnapshot(file: TFile, snap: Snapshot): Promise<void> {
    // Best-effort restore to the snapshot's exact frontmatter values + order (but formatting will normalize)
    const typeValue = snap.typeValue;
    if (typeValue && this.schemas[typeValue]) {
      // If we have schema, ensure schema order if syncOrder enabled; otherwise keep snap order
      const desiredOrder = this.settings.syncOrder ? this.schemas[typeValue].keysOrdered : snap.keysOrdered;
      await this.rewriteFrontmatterByOrder(file, desiredOrder, snap.frontmatterObj, { forceTypeValue: typeValue });
    } else {
      // no schema: just rewrite with snapshot order
      await this.rewriteFrontmatterByOrder(file, snap.keysOrdered, snap.frontmatterObj, {});
    }
  }

  private async rewriteToSchema(
    file: TFile,
    typeValue: string,
    opts: { preserveOverlapValues: boolean; renameHint?: { from: string; to: string } }
  ): Promise<void> {
    const schema = this.schemas[typeValue];
    if (!schema) return;

    const current = await this.buildSnapshot(file);
    const currentObj = current.frontmatterObj;

    // Build new frontmatter values:
    // - keep overlap values
    // - add missing as null
    // - remove non-schema keys
    // - ensure Type equals typeValue
    const desiredKeys = schema.keysOrdered.includes(TYPE_KEY)
      ? schema.keysOrdered
      : [TYPE_KEY, ...schema.keysOrdered];

    const outObj: Record<string, any> = {};

    // optional rename hint (preserve values)
    const renameFrom = opts.renameHint?.from;
    const renameTo = opts.renameHint?.to;

    for (const key of desiredKeys) {
      if (key === TYPE_KEY) {
        outObj[TYPE_KEY] = typeValue;
        continue;
      }

      if (renameFrom && renameTo && key === renameTo && Object.prototype.hasOwnProperty.call(currentObj, renameFrom)) {
        outObj[key] = currentObj[renameFrom];
        continue;
      }

      if (Object.prototype.hasOwnProperty.call(currentObj, key)) {
        outObj[key] = currentObj[key];
      } else {
        outObj[key] = null; // blank
      }
    }

    // When order sync is disabled, we preserve existing file order as much as possible:
    // filter existing order to desired keys, then append missing in schema order.
    let finalOrder: string[] = desiredKeys;
    if (!this.settings.syncOrder) {
      const existingOrder = current.keysOrdered;
      const filtered = existingOrder.filter((k) => desiredKeys.includes(k));
      const missing = desiredKeys.filter((k) => !filtered.includes(k));
      finalOrder = [...filtered, ...missing];
    }

    await this.rewriteFrontmatterByOrder(file, finalOrder, outObj, { forceTypeValue: typeValue });
  }

  private async setTypeValue(file: TFile, typeValue: string): Promise<void> {
    const snap = await this.buildSnapshot(file);
    const obj = { ...snap.frontmatterObj, [TYPE_KEY]: typeValue };
    const order = snap.hasFrontmatter ? snap.keysOrdered : [TYPE_KEY, ...Object.keys(obj)];
    // Ensure Type is in order
    const finalOrder = order.includes(TYPE_KEY) ? order : [TYPE_KEY, ...order];
    await this.rewriteFrontmatterByOrder(file, finalOrder, obj, { forceTypeValue: typeValue });
  }

  private async removeTypeKey(file: TFile): Promise<void> {
    const snap = await this.buildSnapshot(file);
    const obj = { ...snap.frontmatterObj };
    delete obj[TYPE_KEY];
    const order = snap.keysOrdered.filter((k) => k !== TYPE_KEY);
    await this.rewriteFrontmatterByOrder(file, order, obj, {});
  }

  private async rewriteFrontmatterByOrder(
    file: TFile,
    keysOrdered: string[],
    fmObj: Record<string, any>,
    opts: { forceTypeValue?: string }
  ): Promise<void> {
    // suppression so our own modify doesn't prompt
    this.suppressOnce(file.path);

    const current = await this.app.vault.read(file);
    const { bodyText } = this.extractFrontmatter(current);

    // Create object in insertion order
    const orderedObj: Record<string, any> = {};
    for (const key of keysOrdered) {
      if (!Object.prototype.hasOwnProperty.call(fmObj, key)) continue;
      if (key === TYPE_KEY && opts.forceTypeValue) {
        orderedObj[key] = opts.forceTypeValue;
      } else {
        orderedObj[key] = fmObj[key];
      }
    }

    // Ensure Type exists if present in fmObj
    if (Object.prototype.hasOwnProperty.call(fmObj, TYPE_KEY) && !Object.prototype.hasOwnProperty.call(orderedObj, TYPE_KEY)) {
      orderedObj[TYPE_KEY] = opts.forceTypeValue ?? fmObj[TYPE_KEY];
    }

    // YAML stringify
    let yamlText = stringifyYaml(orderedObj);

    // Convert "key: null" lines into "key:" for blanks
    // We don't care about formatting fidelity.
    for (const [k, v] of Object.entries(orderedObj)) {
      if (v === null) {
        const re = new RegExp(`^${this.escapeRegex(k)}:\\s*null\\s*$`, "gm");
        yamlText = yamlText.replace(re, `${k}:`);
      }
    }

    const nextContent = `---\n${yamlText}---\n${bodyText ?? ""}`;

    await this.app.vault.modify(file, nextContent);

    // refresh snapshot after write
    const snap = await this.buildSnapshot(file);
    this.snapshots.set(file.path, snap);
  }

  private escapeRegex(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }
}
