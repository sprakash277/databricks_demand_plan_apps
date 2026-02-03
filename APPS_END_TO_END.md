# End-to-end: Creating a Databricks App

Step-by-step guide to create and run a [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps) (Streamlit, Dash, etc.) from scratch.

---

## 1. Prerequisites

- **Databricks workspace** with Apps enabled (admin may need to enable in **Settings** → **Preview**).
- **Permissions**: You need permission to create apps and to use the resources the app will use (e.g. SQL warehouse, secrets).
- **App code** in a folder with at least:
  - Entry point (e.g. `app.py` for Streamlit).
  - Optional: `app.yaml` (run command and env), `requirements.txt` (Python deps).

---

## 2. Prepare the app folder

### 2.1 Minimum structure

```
my_app/
├── app.py          # Entry point (e.g. Streamlit app)
├── app.yaml        # Optional: command and env
└── requirements.txt  # Optional: pip dependencies
```

### 2.2 `app.py` (example: Streamlit)

- Use a supported framework (Streamlit, Dash, Gradio, etc.).
- To call Databricks SQL, use `databricks-sql-connector` and get the warehouse **HTTP path** from env (e.g. `DATABRICKS_WAREHOUSE_ID` → `/sql/1.0/warehouses/<id>`) or user input.

### 2.3 `app.yaml` (optional)

Defines how the app runs. Example:

```yaml
command:
  - streamlit
  - run
  - app.py
  - --server.headless
  - "true"
env:
  - name: STREAMLIT_GATHER_USAGE_STATS
    value: "false"
  # Bind SQL warehouse (use the resource key you set in the UI):
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql_warehouse
```

- **command**: Overrides default (`python app.py`). For Streamlit: `streamlit run app.py --server.headless true`.
- **env**: Environment variables. Use **valueFrom** to reference an app resource (e.g. SQL warehouse, secret); use **value** for non-sensitive literals.

### 2.4 `requirements.txt` (optional)

List Python dependencies. Example:

```
databricks-sdk
databricks-sql-connector
streamlit
pandas
```

---

## 3. Upload the app to the workspace

**Option A – UI**

1. In the sidebar: **Workspace** → navigate to your user folder (e.g. `/Users/your.email@company.com`).
2. **Right‑click** the folder → **Import**.
3. Select **Folder** and upload your `my_app` folder, or create a folder and drag‑and‑drop the files.

**Option B – CLI (sync)**

```bash
# From the directory that contains my_app/
databricks sync ./my_app /Workspace/Users/your.email@company.com/my_app
```

Use `--watch` to keep syncing on file changes.

---

## 4. Create the app in the UI

1. In the sidebar: **Compute** → **Apps**.
2. Click **Create app**.
3. **Name**: e.g. `my-app`.
4. **Source code path**: Click **Browse** and select the folder you uploaded (e.g. `/Users/your.email@company.com/my_app`).  
   This path is where the app code lives; deployment uses this folder.
5. Click **Create** (or **Next**). You may land on a **Configure** step.

---

## 5. Configure resources (recommended)

Resources (e.g. SQL warehouse, secrets) are configured in the app’s **Configure** step and exposed to the app via env vars.

### 5.1 SQL warehouse (for running SQL from the app)

1. In the app: **Configure** (or **Edit** → **Configure**).
2. **App resources** → **+ Add resource**.
3. Select **SQL warehouse**.
4. Choose a warehouse from the list (same workspace only).
5. Permission: **Can use** (or **Can manage** if the app must change warehouse settings).
6. **Resource key**: e.g. `sql_warehouse` (default). Remember this for `app.yaml`.
7. Save.

In `app.yaml` (in the app folder), add:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql_warehouse
```

(Use the same **resource key** as in the UI.) The app will receive the warehouse ID and can build the HTTP path: `/sql/1.0/warehouses/<id>`.

### 5.2 Secret (for tokens or API keys)

1. **Configure** → **App resources** → **+ Add resource** → **Secret**.
2. Choose **Secret scope** and **Secret key** (e.g. the scope and key where the token is stored).
3. Permission: **Can read**.
4. **Resource key**: e.g. `my_secret_token`.
5. Save.

In `app.yaml`:

```yaml
env:
  - name: MY_SECRET_TOKEN
    valueFrom: my_secret_token
```

Never put secrets in `app.yaml` as plain **value**; always use a secret resource and **valueFrom**.

---

## 6. Deploy the app

1. Open the app in **Compute** → **Apps**.
2. Click **Deploy** (or **Deploy** → **Deploy**).
3. Select the **source code path** (same folder as in step 4).
4. Click **Deploy**.
5. Wait until status is **Running**. If it fails, open **Logs** and fix errors (e.g. missing dependency in `requirements.txt`, wrong `command` in `app.yaml`).

---

## 7. Open and use the app

1. On the app’s page, click **Open** (or the app URL).
2. The app opens in a new tab (e.g. `https://<app-name>-<workspace-id>.<region>.databricksapps.com`).
3. Use the app; the app uses the resources you attached (e.g. SQL warehouse via `DATABRICKS_WAREHOUSE_ID`).

---

## 8. Share the app (optional)

1. **Compute** → **Apps** → your app.
2. Use **Permissions** (or **Share**) to grant **Can use** to groups or users.
3. Users must be in the same Databricks account; they open the app from **Apps** or the link.

---

## 9. Update and redeploy

1. Edit the code in the workspace folder (or sync again via CLI).
2. **Compute** → **Apps** → your app → **Deploy**.
3. Select the same source folder and deploy. The app restarts with the new code.

If you change **only** `app.yaml` or resources, you may still need to **Deploy** again so the runtime picks up the new env or resource bindings.

---

## 10. Checklist (quick reference)

| Step | Action |
|------|--------|
| 1 | Have app code: `app.py`, optional `app.yaml`, `requirements.txt` |
| 2 | Upload folder to Workspace (Import or `databricks sync`) |
| 3 | **Compute** → **Apps** → **Create app**; set name and source code path |
| 4 | **Configure** → Add **SQL warehouse** (and/or **Secret**) resource; note resource keys |
| 5 | In app folder, set `app.yaml` env (e.g. `DATABRICKS_WAREHOUSE_ID` with `valueFrom: sql_warehouse`) |
| 6 | **Deploy**; select same source folder |
| 7 | **Open** app and test |
| 8 | (Optional) **Permissions** → grant **Can use** to others |

---

## 11. Using a SQL warehouse in another workspace

The **SQL warehouse** resource only lists warehouses in the **same** workspace. To use a warehouse in a **different** workspace:

1. Create a **secret** in the app’s workspace that holds a token (PAT) for the **other** workspace.
2. Add that secret as an app **resource** (e.g. key `remote_databricks_token`).
3. In `app.yaml`, set:
   - `REMOTE_WORKSPACE_HOST` = other workspace hostname
   - `REMOTE_HTTP_PATH` = other warehouse HTTP path (e.g. `/sql/1.0/warehouses/<id>`)
   - `REMOTE_DATABRICKS_TOKEN` with `valueFrom: remote_databricks_token`
4. In app code, when these env vars are set, connect using `server_hostname`, `http_path`, and `access_token` (from the env) instead of the local warehouse.

See this repo’s **README** section “Other-workspace: use a SQL warehouse in a different workspace” for a full example.

---

## References

- [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps)
- [Deploy a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy)
- [Add resources to a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/resources)
- [Configure app execution with app.yaml](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/app-runtime)
