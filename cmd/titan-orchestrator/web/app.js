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
        configured: "Configured"
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
        filter_all: "全部",
        filter_online: "在线",
        filter_offline: "离线",
        th_node_id: "节点 ID",
        th_ip: "IP 地址",
        th_version: "版本",
        th_tags: "标签",
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
        configured: "已配置"
    }
};

let state = {
    instances: {},
    lang: localStorage.getItem('titan_lang') || 'en'
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
    supervisorList.innerHTML = '';

    supervisors.forEach(s => {
        const lastSeen = new Date(s.last_heartbeat);
        const isOnline = (now - lastSeen) / 1000 < 60;

        if (filter === 'online' && !isOnline) return;
        if (filter === 'offline' && isOnline) return;

        const row = document.createElement('tr');
        row.innerHTML = `
            <td class="td-id"><code>${s.id.split('-')[0]}...</code></td>
            <td>${s.ip}</td>
            <td><span class="tag tag-outline">${s.version}</span></td>
            <td>${(s.tags || '').split(',').map(tag => tag.trim() ? `<span class="tag">${tag}</span>` : '').join(' ')}</td>
            <td style="color:var(--text-secondary)">${lastSeen.toLocaleString()}</td>
            <td>
                <span class="${isOnline ? 'td-online' : 'td-offline'}">
                    ● ${isOnline ? t.online : t.offline}
                </span>
            </td>
        `;
        supervisorList.appendChild(row);
    });
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
    document.getElementById('inst-params').value = '{\n  "port": 5577\n}';
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
