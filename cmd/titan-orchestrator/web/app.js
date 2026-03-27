// Multi-language Support (i18n)
const i18n = {
    en: {
        report_token: "Report Token",
        connecting: "Connecting...",
        total_instances: "Total Instances",
        configured_entities: "Configured entities",
        online_nodes: "Online Nodes",
        active_heartbeat: "Active heartbeat",
        offline_nodes: "Offline Nodes",
        connection_lost: "Connection lost",
        instances: "Instances",
        supervisors: "Supervisors",
        worker_instances: "Worker Instances",
        active_supervisors: "Active Supervisors",
        add_instance: "+ Add Instance",
        status_label: "Status:",
        filter_all: "All",
        filter_online: "Online",
        filter_offline: "Offline",
        th_node_id: "Node ID",
        th_ip: "IP Address",
        th_version: "Version",
        th_tags: "Tags",
        th_cpu: "CPU / Cores",
        th_memory: "Memory",
        th_disk: "Disk",
        th_network: "Net (rx/tx)",
        th_heartbeat: "Last Heartbeat",
        th_status: "Status",
        footer_text: "&copy; 2026 Filecoin-Titan Orchestrator. Decentralized Control Plane.",
        modal_edit_title: "Edit Instance",
        modal_add_title: "Add New Instance",
        field_enabled: "Enabled",
        field_enabled_tip: "(If OFF, Supervisor will stop the process)",
        field_name: "Instance Name (ID)",
        field_platforms: "Multi-platform Binaries (JSON)",
        field_args: "Launch Arguments (JSON Array)",
        field_pre_stop: "Pre-stop Command (Optional)",
        field_env: "Environment Variables (JSON Object)",
        field_dir: "Custom Instance Directory (Optional)",
        field_tags: "Tags (Comma separated)",
        field_params: "Template Parameters (JSON Object)",
        btn_cancel: "Cancel",
        btn_save: "Save Changes",
        btn_delete: "Delete",
        btn_edit: "Edit",
        login_title: "Titan Login",
        login_user: "Username",
        login_user_ph: "Enter admin username",
        login_pass: "Password",
        login_pass_ph: "Enter admin password",
        btn_login: "Login to Dashboard",
        login_error: "Invalid username or password",
        conf_saved: "Configuration Saved",
        deleted_msg: "Deleted",
        confirm_del: "Are you sure you want to delete ",
        online: "ONLINE",
        offline: "OFFLINE",
        none: "None",
        configured: "Configured",
        th_actions: "Actions",
        edit_node_tags: "Edit Node Tags",
        node_tags_tip: "Server-side tag management mode.",
        node_tags_placeholder: "Add tag and press Enter",
        node_tags_override: "Override Local Tags",
        node_tags_override_tip: "If ON, ignore node's local --tags flag",
        node_tags_tip_merge: "Merge Mode: Server tags are added to local --tags.",
        node_tags_tip_override: "Override Mode: Server tags completely replace local --tags.",
        btn_save_tags: "Save Tags",
        tags_saved: "Tags saved",
        local_tags: "Local",
        server_tags: "Server",
        btn_view_instances: "View Services",
        modal_instances_title: "Services on Node",
        th_instance_name: "Service Name",
        th_instance_status: "Status / Hash",
        th_instance_crashes: "Crashes",
        btn_view_log: "View Log",
        modal_log_title: "Error Log Details",
        no_instances: "No services dispatched to this node.",
        status_missing: "MISSING",
        pg_prev: "Prev",
        pg_next: "Next",
        pg_page: "Page",
        pg_of: "of",
        pg_total: "Total",
        pg_items_per_page: "per page"
    },
    cn: {
        report_token: "报备令牌",
        connecting: "连接中...",
        total_instances: "实例总数",
        configured_entities: "已配置的业务实体",
        online_nodes: "在线节点",
        active_heartbeat: "活跃心跳中",
        offline_nodes: "离线节点",
        connection_lost: "失去连接",
        instances: "实例配置",
        supervisors: "节点列表",
        worker_instances: "工作实例",
        active_supervisors: "活跃节点",
        add_instance: "+ 添加实例",
        status_label: "状态:",
        status_missing: "缺失",
        filter_all: "全部",
        filter_online: "在线",
        filter_offline: "离线",
        th_node_id: "节点 ID",
        th_ip: "IP 地址",
        th_version: "版本",
        th_tags: "标签",
        th_cpu: "CPU / 核心",
        th_memory: "内存",
        th_disk: "磁盘",
        th_network: "网络 (收/发)",
        th_heartbeat: "最近心跳",
        th_status: "状态",
        footer_text: "&copy; 2026 Filecoin-Titan 调度器. 去中心化控制面板.",
        modal_edit_title: "编辑实例",
        modal_add_title: "添加新实例",
        field_enabled: "启用状态",
        field_enabled_tip: "(如果关闭，Supervisor 将停止该进程)",
        field_name: "实例名称 (ID)",
        field_platforms: "多平台二进制配置 (JSON)",
        field_args: "启动参数 (JSON 数组)",
        field_pre_stop: "预停止命令 (可选)",
        field_env: "环境变量 (JSON 对象)",
        field_dir: "自定义实例目录 (可选)",
        field_tags: "标签 (逗号分隔)",
        field_params: "模板参数 (JSON 对象)",
        btn_cancel: "取消",
        btn_save: "保存更改",
        btn_delete: "删除",
        btn_edit: "编辑",
        login_title: "Titan 登录",
        login_user: "用户名",
        login_user_ph: "输入管理员用户名",
        login_pass: "密码",
        login_pass_ph: "输入管理员密码",
        btn_login: "登录后台",
        login_error: "用户名或密码错误",
        conf_saved: "配置已保存",
        deleted_msg: "已删除",
        confirm_del: "确认要删除吗 ",
        online: "在线",
        offline: "离线",
        none: "无",
        configured: "已配置",
        th_actions: "操作",
        edit_node_tags: "编辑标签",
        node_tags_tip: "服务端标签管理模式设置。",
        node_tags_placeholder: "输入标签后按 Enter",
        node_tags_override: "覆盖本地标签",
        node_tags_override_tip: "开启后，将完全忽略节点的本地 --tags 配置",
        node_tags_tip_merge: "合并模式：服务端标签将与节点本地 --tags 累加。",
        node_tags_tip_override: "覆盖模式：服务端标签将完全替代节点本地的 --tags。",
        btn_save_tags: "保存标签",
        tags_saved: "标签已保存",
        local_tags: "本地",
        server_tags: "服务器",
        btn_view_instances: "查看服务",
        modal_instances_title: "节点下发服务列表",
        th_instance_name: "服务名称",
        th_instance_status: "状态 / Hash",
        th_instance_crashes: "崩溃次数",
        btn_view_log: "查看日志",
        modal_log_title: "错误详情日志",
        no_instances: "该节点暂未下发任何服务。",
        pg_prev: "上一页",
        pg_next: "下一页",
        pg_page: "第",
        pg_of: "页 / 共",
        pg_total: "共计",
        pg_items_per_page: "条/页"
    }
};

window.onerror = function(msg, url, line, col, error) {
    console.error("Global Error:", msg, "at", line, ":", col);
    if (window.showToast) window.showToast("JS Error: " + msg);
    return false;
};

let state = {
    instances: {},
    lang: localStorage.getItem('titan_lang') || 'en',
    supPage: 1,
    supPageSize: 10
};

const instanceGrid = document.getElementById('instance-grid');
const modalOverlay = document.getElementById('modal-overlay');
const instanceForm = document.getElementById('instance-form');
const toast = document.getElementById('toast');
const serverStatus = document.getElementById('server-status');

const loginOverlay = document.getElementById('login-overlay');
const loginForm = document.getElementById('login-form');
const loginError = document.getElementById('login-error');

let editingId = null;
let supervisors = [];

const supervisorList = document.getElementById('supervisor-list');
const statusFilter = document.getElementById('status-filter');

// i18n Logic
function updateLanguage() {
    const langData = i18n[state.lang];
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        if (langData[key]) {
            el.innerHTML = langData[key];
        }
    });
    document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
        const key = el.getAttribute('data-i18n-placeholder');
        if (langData[key]) {
            el.setAttribute('placeholder', langData[key]);
        }
    });

    // Update active state of buttons
    document.querySelectorAll('.lang-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.lang === state.lang);
    });
}

document.querySelectorAll('.lang-btn').forEach(btn => {
    btn.onclick = () => {
        state.lang = btn.dataset.lang;
        localStorage.setItem('titan_lang', state.lang);
        updateLanguage();
        render();
        renderSupervisors();
    };
});

// Auth Management
const getToken = () => localStorage.getItem('titan_token');
const setToken = (token) => localStorage.setItem('titan_token', token);
const clearToken = () => {
    localStorage.removeItem('titan_token');
    showLogin();
};
const showLogin = () => loginOverlay.classList.remove('hidden');
const hideLogin = () => loginOverlay.classList.add('hidden');

async function fetchWithAuth(url, options = {}) {
    const token = getToken();
    const headers = options.headers || {};
    if (token) headers['Authorization'] = `Bearer ${token}`;
    const resp = await fetch(url, { ...options, headers });
    if (resp.status === 401) {
        clearToken();
        throw new Error('Unauthorized');
    }
    return resp;
}

loginForm.onsubmit = async (e) => {
    e.preventDefault();
    const user = document.getElementById('login-user').value;
    const pass = document.getElementById('login-pass').value;
    try {
        const resp = await fetch('/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user, pass })
        });
        if (resp.ok) {
            const data = await resp.json();
            setToken(data.token);
            hideLogin();
            loginError.classList.add('hidden');
            fetchData();
        } else {
            loginError.classList.remove('hidden');
        }
    } catch (err) {
        loginError.classList.remove('hidden');
    }
};

// Tab Management
const tabButtons = document.querySelectorAll('.tab-btn');
const tabContents = document.querySelectorAll('.tab-content');

tabButtons.forEach(btn => {
    btn.onclick = () => {
        const target = btn.dataset.tab;
        tabButtons.forEach(b => b.classList.toggle('active', b === btn));
        tabContents.forEach(c => c.classList.toggle('active', c.id === target));
        if (target === 'supervisors-tab') fetchSupervisors();
    };
});

// Stats Calculation
function updateStats() {
    const langData = i18n[state.lang];
    const total = Object.keys(state.instances).length;
    const now = new Date();
    const online = supervisors.filter(s => (now - new Date(s.last_heartbeat)) / 1000 < 60).length;
    const offline = supervisors.length - online;

    document.getElementById('stats-total').textContent = total;
    document.getElementById('stats-online').textContent = online;
    document.getElementById('stats-offline').textContent = Math.max(0, offline);
}

// Data Fetching
async function fetchData() {
    try {
        const resp = await fetchWithAuth('/api/config');
        const data = await resp.json();
        state.instances = data.instances || {};

        const secretVal = document.getElementById('report-secret-val');
        const secretContainer = document.getElementById('report-secret-container');
        if (data.report_secret) {
            secretVal.textContent = data.report_secret;
            secretContainer.classList.remove('hidden');
        } else {
            secretContainer.classList.add('hidden');
        }

        render();
        updateStats();
        serverStatus.textContent = i18n[state.lang].online || 'Online';
        serverStatus.classList.add('online');
    } catch (err) {
        if (err.message !== 'Unauthorized') {
            serverStatus.textContent = i18n[state.lang].offline || 'Offline';
            serverStatus.classList.remove('online');
        }
    }
}

async function fetchSupervisors() {
    try {
        const res = await fetchWithAuth('/api/supervisors');
        if (res.ok) {
            supervisors = await res.json();
            renderSupervisors();
            updateStats();
        }
    } catch (err) { }
}

function render() {
    const t = i18n[state.lang];
    instanceGrid.innerHTML = '';

    Object.entries(state.instances).forEach(([id, info]) => {
        const card = document.createElement('div');
        const isEnabled = info.enabled !== false;
        card.className = `card ${!isEnabled ? 'disabled' : ''}`;

        const paramsStr = JSON.stringify(info.params || {}, null, 2);
        const platformsStr = info.platforms ? Object.keys(info.platforms).map(p => `<span class="tag tag-outline">${p}</span>`).join(' ') : 'Default';

        card.innerHTML = `
            <div class="card-header">
                <div class="card-title">${id} ${!isEnabled ? `<span class="tag" style="background:var(--accent-red);color:white;">OFF</span>` : ''}</div>
                <button class="btn btn-secondary btn-sm" onclick="editInstance('${id}')">${t.btn_edit}</button>
            </div>
            <div class="card-meta-list">
                <div class="meta-item"><b>OS:</b> ${platformsStr}</div>
                ${info.instance_dir ? `<div class="meta-item"><b>DIR:</b> ${info.instance_dir}</div>` : ''}
                <div class="meta-item"><b>ENV:</b> ${JSON.stringify(info.env || {}) !== '{}' ? t.configured : t.none}</div>
            </div>
            <div class="card-tags">
                ${(info.tags || []).map(tag => `<span class="tag">${tag}</span>`).join('')}
            </div>
            <div class="card-params">${paramsStr}</div>
            <div class="card-actions">
                <button class="btn btn-danger btn-sm" onclick="deleteInstance('${id}')">${t.btn_delete}</button>
            </div>
        `;
        instanceGrid.appendChild(card);
    });
}

function renderSupervisors() {
    const t = i18n[state.lang];
    const filter = statusFilter.value;
    const now = new Date();
    
    // 1. Filter
    let filtered = supervisors.filter(s => {
        const lastSeen = new Date(s.last_heartbeat);
        const isOnline = (now - lastSeen) / 1000 < 60;
        if (filter === 'online' && !isOnline) return false;
        if (filter === 'offline' && isOnline) return false;
        return true;
    });

    // 2. Stable Sort (Online first, then by ID)
    filtered.sort((a, b) => {
        const aOnline = (now - new Date(a.last_heartbeat)) / 1000 < 60;
        const bOnline = (now - new Date(b.last_heartbeat)) / 1000 < 60;
        
        if (aOnline !== bOnline) {
            return aOnline ? -1 : 1; // Online first
        }
        return a.id.localeCompare(b.id); // ID stable
    });

    // 3. Pagination Logic
    const totalItems = filtered.length;
    const totalPages = Math.ceil(totalItems / state.supPageSize) || 1;
    if (state.supPage > totalPages) state.supPage = totalPages;

    const start = (state.supPage - 1) * state.supPageSize;
    const slice = filtered.slice(start, start + state.supPageSize);

    supervisorList.innerHTML = '';

    // 4. Render Table Rows
    slice.forEach(s => {
        const lastSeen = new Date(s.last_heartbeat);
        const isOnline = (now - lastSeen) / 1000 < 60;

        // Formatting Helpers
        const formatBytes = (bytes) => {
            if (bytes === 0 || !bytes) return "0 B";
            const k = 1024, sizes = ["B", "KB", "MB", "GB", "TB"];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
        };

        const memText = s.total_memory_bytes ? `${formatBytes(s.used_memory_bytes)} / ${formatBytes(s.total_memory_bytes)}` : t.none;
        const diskText = s.total_disk_bytes ? `${formatBytes(s.used_disk_bytes)} / ${formatBytes(s.total_disk_bytes)}` : t.none;
        const cpuText = s.cpu_cores ? `${s.cpu_percent.toFixed(1)}% (${s.cpu_cores}C)` : t.none;
        const netText = (s.net_rx_bytes_per_sec || s.net_tx_bytes_per_sec) ?
            `<span style="color:var(--accent-green)">↓${formatBytes(s.net_rx_bytes_per_sec)}/s</span> <span style="color:var(--accent-blue)">↑${formatBytes(s.net_tx_bytes_per_sec)}/s</span>` : t.none;

        const localTagsHtml = (s.tags || '').split(',').map(tag => tag.trim()
            ? `<span class="tag tag-outline" title="${t.local_tags}">${tag.trim()}</span>`
            : '').join(' ');

        const serverTagsHtml = (s._serverTags || []).map(tag =>
            `<span class="tag" style="background:var(--accent-blue);color:white;border:none" title="${t.server_tags}">${tag}</span>`
        ).join(' ');

        const row = document.createElement('tr');
        row.id = `sv-row-${s.id}`;
        row.innerHTML = `
            <td class="td-id"><code title="${s.id}">${s.id.substring(0, 12)}...</code></td>
            <td>${s.ip}</td>
            <td style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem">${cpuText}</td>
            <td style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem">${memText}</td>
            <td style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem">${diskText}</td>
            <td style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem">${netText}</td>
            <td><span class="tag tag-outline">${s.version}</span></td>
            <td class="td-tags">
                ${localTagsHtml}
                ${serverTagsHtml}
            </td>
            <td style="color:var(--text-secondary)">${lastSeen.toLocaleString()}</td>
            <td>
                <span class="${isOnline ? 'td-online' : 'td-offline'}">
                    ● ${isOnline ? t.online : t.offline}
                </span>
            </td>
            <td>
                <button class="btn btn-primary btn-sm" onclick="if(window.openInstanceViewer) window.openInstanceViewer('${s.id}')" title="${t.btn_view_instances}">📊</button>
                <button class="btn btn-secondary btn-sm" onclick="openTagEditor('${s.id}')" title="${t.edit_node_tags}">🏷️</button>
            </td>
        `;
        supervisorList.appendChild(row);
    });

    // 5. Render Pagination Controls
    const parent = supervisorList.closest('.table-container');
    let pgContainer = parent.querySelector('.pagination-container');
    if (!pgContainer) {
        pgContainer = document.createElement('div');
        pgContainer.className = 'pagination-container';
        parent.appendChild(pgContainer);
    }

    pgContainer.innerHTML = `
        <div>
            ${t.pg_total} <b>${totalItems}</b>
            <select class="pg-size-select" style="margin-left: 15px" onchange="window.changeSupPageSize(this.value)">
                <option value="10" ${state.supPageSize == 10 ? 'selected' : ''}>10 ${t.pg_items_per_page}</option>
                <option value="20" ${state.supPageSize == 20 ? 'selected' : ''}>20 ${t.pg_items_per_page}</option>
                <option value="50" ${state.supPageSize == 50 ? 'selected' : ''}>50 ${t.pg_items_per_page}</option>
                <option value="100" ${state.supPageSize == 100 ? 'selected' : ''}>100 ${t.pg_items_per_page}</option>
            </select>
        </div>
        <div class="pagination-controls">
            <button class="pg-btn" onclick="window.changeSupPage(-1)" ${state.supPage <= 1 ? 'disabled' : ''}>${t.pg_prev}</button>
            <span class="pg-info">${t.pg_page} <b>${state.supPage}</b> ${t.pg_of} <b>${totalPages}</b></span>
            <button class="pg-btn" onclick="window.changeSupPage(1)" ${state.supPage >= totalPages ? 'disabled' : ''}>${t.pg_next}</button>
        </div>
    `;
}

// Pagination Helpers
window.changeSupPage = (delta) => {
    state.supPage += delta;
    renderSupervisors();
};

window.changeSupPageSize = (size) => {
    state.supPageSize = parseInt(size);
    state.supPage = 1;
    renderSupervisors();
};

function getEffectiveTags(s) {
    const server = Array.isArray(s._serverTags) ? s._serverTags : [];
    if (s.tags_override) return server.map(t => t.toLowerCase().trim());
    const local = (s.tags || '').split(',').map(t => t.toLowerCase().trim()).filter(t => t);
    return [...new Set([...server.map(t => t.toLowerCase().trim()), ...local])];
}

function isTagAllowed(tag, allowedTags) {
    if (!tag) return true;
    const lowerTag = tag.toLowerCase().trim();
    if (lowerTag.startsWith('!')) {
        const base = lowerTag.substring(1).trim();
        return !allowedTags.includes(base);
    }
    return allowedTags.includes(lowerTag);
}

// ── Instance Viewer / Log Viewer ───────────────────────────────────────────

function initInstanceViewer() {
    if (document.getElementById('instance-viewer-modal')) return;
    
    console.log("Initializing Instance Viewer Modals...");
    const container = document.createElement('div');
    container.id = 'instance-viewer-container';
    container.innerHTML = `
    <style>
        .modal-overlay {
            position: fixed; top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0, 0, 0, 0.85); backdrop-filter: blur(8px);
            display: flex; align-items: center; justify-content: center;
            z-index: 9999;
        }
        .modal-overlay.hidden { display: none !important; }
    </style>
    <div id="instance-viewer-modal" class="modal-overlay hidden" style="z-index:9999">
        <div class="modal" style="max-width:850px; width:95%; padding:1.5rem">
            <div class="modal-header" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem">
                <h3 id="instance-viewer-title" class="modal-title" style="margin:0; font-size:1.25rem"></h3>
                <button class="modal-close" style="background:none; border:none; color:var(--text-secondary); cursor:pointer; font-size:1.5rem" onclick="document.getElementById('instance-viewer-modal').classList.add('hidden')">✕</button>
            </div>
            <div class="table-container" style="margin-top:10px; max-height:500px; overflow-y:auto; background: rgba(0,0,0,0.2); border-radius:12px; border:1px solid var(--border-color)">
                <table class="stats-table" style="width:100%; border-collapse:collapse;">
                    <thead>
                        <tr style="background:rgba(0,0,0,0.3)">
                            <th id="th-name" style="padding:12px; text-align:left; color:var(--text-secondary)">Name</th>
                            <th id="th-status" style="padding:12px; text-align:left; color:var(--text-secondary)">Status / Hash</th>
                            <th id="th-crashes" style="padding:12px; text-align:center; color:var(--text-secondary)">Crashes</th>
                            <th id="th-log" style="padding:12px; text-align:center; color:var(--text-secondary)">Actions</th>
                        </tr>
                    </thead>
                    <tbody id="instance-viewer-list"></tbody>
                </table>
            </div>
            <div class="modal-footer" style="margin-top:20px; text-align:right;">
                <button class="btn btn-secondary" onclick="document.getElementById('instance-viewer-modal').classList.add('hidden')" id="btn-close-viewer">Close</button>
            </div>
        </div>
    </div>
    <div id="log-viewer-modal" class="modal-overlay hidden" style="z-index:10000">
        <div class="modal" style="max-width:800px; width:90%; background:#0a0a0a; padding:1.5rem">
            <div class="modal-header" style="display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid #333; padding-bottom:10px; margin-bottom:1rem">
                <h3 id="log-viewer-title" class="modal-title" style="margin:0; color:var(--accent-red)">Error Log</h3>
                <button class="modal-close" style="background:none; border:none; color:var(--text-secondary); cursor:pointer; font-size:1.5rem" onclick="document.getElementById('log-viewer-modal').classList.add('hidden')">✕</button>
            </div>
            <pre id="log-content" style="background:#000; color:#00ff00; padding:15px; border-radius:8px; font-family:'JetBrains Mono','Fira Code',monospace; font-size:0.85rem; overflow-x:auto; white-space:pre-wrap; margin-top:10px; max-height:500px; border:1px solid #444; line-height:1.5"></pre>
            <div class="modal-footer" style="margin-top:20px; text-align:right;">
                <button class="btn btn-secondary" onclick="document.getElementById('log-viewer-modal').classList.add('hidden')">Close</button>
            </div>
        </div>
    </div>`;
    document.body.appendChild(container);
}

// Global scope expose
window.openInstanceViewer = function(nodeId) {
    try {
        console.log("Triggering openInstanceViewer for:", nodeId);
        initInstanceViewer(); // Ensure modals exist
        
        const t = i18n[state.lang];
        const sup = supervisors.find(s => s.id === nodeId);
        if (!sup) {
            console.warn("Node data not found in local cache:", nodeId);
            if (window.showToast) window.showToast("Error: Node data not found");
            return;
        }

        document.getElementById('instance-viewer-title').textContent = `${t.modal_instances_title}: ${nodeId.substring(0, 12)}`;
        document.getElementById('th-name').textContent = t.th_instance_name;
        document.getElementById('th-status').textContent = t.th_instance_status;
        document.getElementById('th-crashes').textContent = t.th_instance_crashes;
        document.getElementById('th-log').textContent = t.th_actions;
        document.getElementById('btn-close-viewer').textContent = t.btn_cancel;

        const list = document.getElementById('instance-viewer-list');
        list.innerHTML = '';

        const effectiveTags = getEffectiveTags(sup);
        const expected = [];

        // 2. Derive Expected Instances based on global topology and node tags
        Object.entries(state.instances).forEach(([id, info]) => {
            // Check if instance tags match node tags
            const instTags = Array.isArray(info.tags) ? info.tags : (info.tags || "").split(",").map(t => t.trim()).filter(t => t);
            
            if (instTags.length > 0) {
                const match = instTags.some(t => isTagAllowed(t, effectiveTags));
                if (!match) return; // Not for this node
            }

            // check enabled state
            if (info.enabled === false) return;

            // Find if reported
            const reported = (sup.instances || []).find(i => i.name === id);
            expected.push({
                name: id,
                status: reported ? reported.status : 'missing',
                hash: reported ? reported.hash : 'N/A',
                crash_count: reported ? reported.crash_count : 0,
                last_error: reported ? reported.last_error : null
            });
        });

        console.log("Expected instances for node:", expected.length);
        
        if (expected.length === 0) {
            list.innerHTML = `<tr><td colspan="4" style="padding:60px; text-align:center; opacity:0.5; font-style:italic;">${t.no_instances}</td></tr>`;
        } else {
            expected.forEach((inst, idx) => {
                const row = document.createElement('tr');
                row.style.borderBottom = "1px solid #222";
                
                let statusClass = 'td-offline';
                let statusText = inst.status.toUpperCase();

                if (inst.status === 'running') statusClass = 'td-online';
                else if (['downloading', 'starting'].includes(inst.status)) statusClass = 'td-warning';
                else if (inst.status === 'missing') {
                    statusClass = 'td-offline'; // We'll rely on text to show MISSING
                    statusText = t.status_missing;
                }

                const hashShort = inst.hash && inst.hash !== 'N/A' ? inst.hash.substring(0, 8) : 'N/A';

                row.innerHTML = `
                    <td style="padding:12px; font-weight:600; color:${inst.isExtra ? 'var(--accent-blue)' : 'var(--text-primary)'}">${inst.name}${inst.isExtra ? ' <small>(?)</small>' : ''}</td>
                    <td style="padding:12px">
                        <div class="${statusClass}" style="margin-bottom:4px; font-size:0.85rem; font-weight:bold; color:${inst.status === 'missing' ? 'var(--accent-red)' : ''}">● ${statusText}</div>
                        <code style="font-size:0.75rem; opacity:0.5; background:rgba(255,255,255,0.05); padding:2px 4px; border-radius:4px">${hashShort}</code>
                    </td>
                    <td style="padding:12px; text-align:center; font-family:'JetBrains Mono'">${inst.crash_count}</td>
                    <td style="padding:12px; text-align:center">
                        ${inst.last_error ? `<button class="btn btn-danger btn-sm" data-nodeid="${nodeId}" data-instuuid="${inst.name}" onclick="viewLogByUUID(this)">${t.btn_view_log}</button>` : '<span style="opacity:0.3">-</span>'}
                    </td>
                `;
                list.appendChild(row);
            });
        }

        document.getElementById('instance-viewer-modal').classList.remove('hidden');
    } catch (e) {
        console.error("Critical error in openInstanceViewer:", e);
        if (window.showToast) window.showToast("Viewer Error: " + e.message);
    }
};

window.viewLogByUUID = (btn) => {
    const nodeId = btn.dataset.nodeid;
    const instName = btn.dataset.instuuid;
    const t = i18n[state.lang];
    const sup = supervisors.find(s => s.id === nodeId);
    if (!sup) return;

    const inst = (sup.instances || []).find(i => i.name === instName);
    if (!inst) return;

    document.getElementById('log-viewer-title').textContent = `${t.modal_log_title}: ${inst.name}`;
    document.getElementById('log-content').textContent = inst.last_error;
    document.getElementById('log-viewer-modal').classList.remove('hidden');
};

// ── Node Tag Editor ─────────────────────────────────────────────────────────

// Inject the tag-editor modal once into the DOM
if (!document.getElementById('tag-editor-modal')) {
    const el = document.createElement('div');
    el.innerHTML = `
    <div id="tag-editor-modal" class="modal-overlay hidden" style="z-index:200">
        <div class="modal" style="max-width:480px">
            <div class="modal-header">
                <h2 id="tag-editor-title" class="modal-title"></h2>
                <button class="modal-close" id="close-tag-editor">✕</button>
            </div>
            <div style="display:flex;align-items:center;gap:10px;margin:12px 0;padding:8px;background:rgba(255,255,255,0.05);border-radius:6px">
                <input type="checkbox" id="tag-override-chk" style="width:auto;cursor:pointer" />
                <label for="tag-override-chk" id="tag-override-label" style="margin-bottom:0;cursor:pointer;font-weight:600"></label>
            </div>
            <p id="tag-override-tip" style="font-size:0.75rem;opacity:0.7;margin-bottom:12px"></p>
            <div id="tag-chips" style="display:flex;flex-wrap:wrap;gap:6px;min-height:36px;margin-bottom:12px"></div>
            <div style="display:flex;gap:8px">
                <input id="tag-input" type="text" class="form-input" style="flex:1" />
                <button class="btn btn-secondary" id="tag-add-btn">+</button>
            </div>
            <div class="modal-footer" style="margin-top:16px">
                <button class="btn btn-secondary" id="cancel-tag-editor"></button>
                <button class="btn btn-primary" id="save-tag-editor"></button>
            </div>
        </div>
    </div>`;
    document.body.appendChild(el.firstElementChild);

    document.getElementById('close-tag-editor').onclick =
        document.getElementById('cancel-tag-editor').onclick = () =>
            document.getElementById('tag-editor-modal').classList.add('hidden');

    document.getElementById('tag-add-btn').onclick = addTagChip;
    document.getElementById('tag-input').addEventListener('keydown', e => {
        if (e.key === 'Enter') { e.preventDefault(); addTagChip(); }
    });
}

let _editingNodeId = null;

function addTagChip() {
    const input = document.getElementById('tag-input');
    const val = input.value.trim();
    if (!val) return;
    input.value = '';
    const chips = document.getElementById('tag-chips');
    // Prevent duplicates
    if ([...chips.querySelectorAll('[data-tag]')].some(c => c.dataset.tag === val)) return;
    const chip = document.createElement('span');
    chip.className = 'tag';
    chip.dataset.tag = val;
    chip.style.cursor = 'pointer';
    chip.innerHTML = `${val} <span style="opacity:.6">✕</span>`;
    chip.onclick = () => chip.remove();
    chips.appendChild(chip);
}

async function openTagEditor(nodeId) {
    const t = i18n[state.lang];
    _editingNodeId = nodeId;

    document.getElementById('tag-editor-title').textContent = `${t.edit_node_tags}: ${nodeId.substring(0, 12)}...`;
    document.getElementById('tag-override-label').textContent = t.node_tags_override;
    document.getElementById('tag-override-tip').textContent = t.node_tags_override_tip;
    document.getElementById('tag-input').placeholder = t.node_tags_placeholder;
    document.getElementById('cancel-tag-editor').textContent = t.btn_cancel;
    document.getElementById('save-tag-editor').textContent = t.btn_save_tags;

    const chk = document.getElementById('tag-override-chk');
    chk.checked = false;

    // Load current server tags
    const chips = document.getElementById('tag-chips');
    chips.innerHTML = '<span style="opacity:.4;font-size:.8rem">Loading...</span>';
    document.getElementById('tag-editor-modal').classList.remove('hidden');

    try {
        const res = await fetchWithAuth(`/api/node-tags/${nodeId}`);
        const data = res.ok ? await res.json() : { tags: [], override: false };
        const tags = data.tags || [];
        const override = data.override || false;

        chk.checked = override;

        chips.innerHTML = '';
        tags.forEach(tag => {
            const chip = document.createElement('span');
            chip.className = 'tag';
            chip.dataset.tag = tag;
            chip.style.cursor = 'pointer';
            chip.innerHTML = `${tag} <span style="opacity:.6">✕</span>`;
            chip.onclick = () => chip.remove();
            chips.appendChild(chip);
        });
    } catch (e) {
        chips.innerHTML = '<span style="color:red">Failed to load tags</span>';
    }

    document.getElementById('save-tag-editor').onclick = saveNodeTags;
}

async function saveNodeTags() {
    if (!_editingNodeId) return;
    const t = i18n[state.lang];
    const chips = document.getElementById('tag-chips');
    const tags = [...chips.querySelectorAll('[data-tag]')].map(c => c.dataset.tag);
    const override = document.getElementById('tag-override-chk').checked;

    try {
        const res = await fetchWithAuth(`/api/node-tags/${_editingNodeId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tags, override })
        });
        if (res.ok) {
            renderSupervisors();
            document.getElementById('tag-editor-modal').classList.add('hidden');
            showToast(t.tags_saved);
        } else {
            showToast('Error: ' + res.status);
        }
    } catch (e) {
        showToast('Request failed');
    }
}

// Actions
window.editInstance = (id) => {
    editingId = id;
    const info = state.instances[id];
    const t = i18n[state.lang];
    document.getElementById('modal-title').textContent = t.modal_edit_title;
    document.getElementById('inst-name').value = id;
    document.getElementById('inst-name').disabled = true;
    document.getElementById('inst-enabled').checked = info.enabled !== false;
    document.getElementById('inst-dir').value = info.instance_dir || '';
    document.getElementById('inst-args').value = JSON.stringify(info.args || [], null, 2);
    document.getElementById('inst-pre-stop').value = JSON.stringify(info.pre_stop_command || [], null, 2);
    document.getElementById('inst-env').value = JSON.stringify(info.env || {}, null, 2);
    document.getElementById('inst-tags').value = (info.tags || []).join(', ');
    document.getElementById('inst-params').value = JSON.stringify(info.params || {}, null, 2);
    document.getElementById('inst-platforms').value = JSON.stringify(info.platforms || {}, null, 2);
    modalOverlay.classList.remove('hidden');
};

document.getElementById('add-instance-btn').onclick = () => {
    editingId = null;
    const t = i18n[state.lang];
    document.getElementById('modal-title').textContent = t.modal_add_title;
    document.getElementById('inst-name').value = '';
    document.getElementById('inst-name').disabled = false;
    document.getElementById('inst-enabled').checked = true;
    document.getElementById('inst-dir').value = '';
    document.getElementById('inst-args').value = '[\n  "--config",\n  "{config}"\n]';
    document.getElementById('inst-pre-stop').value = '[]';
    document.getElementById('inst-env').value = '{}';
    document.getElementById('inst-tags').value = '';
    document.getElementById('inst-params').value = '{}';
    document.getElementById('inst-platforms').value = '{}';
    modalOverlay.classList.remove('hidden');
};

document.getElementById('close-modal').onclick = () => modalOverlay.classList.add('hidden');

instanceForm.onsubmit = async (e) => {
    e.preventDefault();
    const id = document.getElementById('inst-name').value;
    try {
        const updated = {
            enabled: document.getElementById('inst-enabled').checked,
            instance_dir: document.getElementById('inst-dir').value,
            args: JSON.parse(document.getElementById('inst-args').value),
            pre_stop_command: JSON.parse(document.getElementById('inst-pre-stop').value),
            env: JSON.parse(document.getElementById('inst-env').value),
            tags: document.getElementById('inst-tags').value.split(',').map(t => t.trim()).filter(t => t !== ""),
            params: JSON.parse(document.getElementById('inst-params').value),
            platforms: JSON.parse(document.getElementById('inst-platforms').value)
        };
        state.instances[id] = updated;
        await saveToServer();
        modalOverlay.classList.add('hidden');
        showToast(i18n[state.lang].conf_saved);
    } catch (err) {
        alert('Invalid JSON Input');
    }
};

window.deleteInstance = async (id) => {
    const t = i18n[state.lang];
    if (!confirm(t.confirm_del + id + '?')) return;
    delete state.instances[id];
    await saveToServer();
    showToast(t.deleted_msg);
};

async function saveToServer() {
    try {
        await fetchWithAuth('/api/config', {
            method: 'POST',
            body: JSON.stringify(state)
        });
        render();
        updateStats();
    } catch (err) { }
}

function showToast(msg) {
    toast.textContent = msg;
    toast.classList.remove('hidden');
    setTimeout(() => toast.classList.add('hidden'), 3000);
}

statusFilter.onchange = renderSupervisors;

// Initial Load
updateLanguage();
if (getToken()) fetchData(); else showLogin();

// Background Refresh
setInterval(() => { if (getToken()) fetchData(); }, 30000);
setInterval(() => {
    if (getToken() && !document.getElementById('supervisors-tab').classList.contains('hidden')) {
        fetchSupervisors();
    }
}, 10000);

// Final check and initialization
if (typeof initInstanceViewer === 'function') initInstanceViewer();
console.log("Titan Orchestrator UI: app.js fully loaded.");
