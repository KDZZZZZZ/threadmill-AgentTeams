(function () {
  "use strict";

  var PHASES = ["plan", "execute", "verify"];
  var PHASE_LABELS = {
    plan: "等待",
    execute: "执行与暂停",
    verify: "已完成"
  };

  var state = null;
  var selectedEndpointId = null;
  var selectedInspector = null;
  var memoryScope = "current";
  var eventSource = null;
  var streamConnected = false;
  var reconnectTimer = null;
  var inspectorRequest = 0;
  var capacityBusy = false;
  var managerBusy = false;
  var localEvents = [];
  var els = {};

  document.addEventListener("DOMContentLoaded", function () {
    cacheElements();
    bindEvents();
    setConnection("warn", "正在连接");
    fetchState();
    connectEvents();
  });

  function cacheElements() {
    [
      "liveDot", "liveText", "revisionText", "lastSyncText",
      "decreaseCapacity", "increaseCapacity", "desiredCapacity", "capacityRevision",
      "metricDesired", "metricHealthy", "metricActive", "metricWaiting",
      "graphSummary", "graphList", "managerMessages", "selectedEndpointBadge",
      "selectedEndpointContext", "messageForm", "managerInput", "managerSubmit",
      "inspectorEndpoint", "inspectorMeta", "subscriptionsList", "effectiveUnionList",
      "contextSlice", "memoryBuffer", "memoryCurrent", "memoryTask",
      "errorBox", "eventList"
    ].forEach(function (id) {
      els[id] = document.getElementById(id);
    });
  }

  function bindEvents() {
    els.decreaseCapacity.addEventListener("click", function () {
      changeCapacity(-1);
    });
    els.increaseCapacity.addEventListener("click", function () {
      changeCapacity(1);
    });
    els.messageForm.addEventListener("submit", sendManagerMessage);
    document.querySelectorAll("[data-prompt]").forEach(function (button) {
      button.addEventListener("click", function () {
        sendManagerText(button.getAttribute("data-prompt") || "", false);
      });
    });
    els.memoryCurrent.addEventListener("click", function () {
      setMemoryScope("current");
    });
    els.memoryTask.addEventListener("click", function () {
      setMemoryScope("task");
    });
  }

  function setConnection(kind, text) {
    els.liveDot.className = "status-dot status-" + kind;
    els.liveText.textContent = text;
  }

  function markSynced() {
    els.lastSyncText.textContent = "同步 " + new Date().toLocaleTimeString("zh-CN", { hour12: false });
    document.body.classList.remove("is-updating");
    window.requestAnimationFrame(function () {
      document.body.classList.add("is-updating");
      window.setTimeout(function () {
        document.body.classList.remove("is-updating");
      }, 180);
    });
  }

  function showError(message) {
    els.errorBox.hidden = !message;
    els.errorBox.textContent = message || "";
  }

  function addLocalEvent(text, kind) {
    localEvents.unshift({
      text: text,
      kind: kind || "info",
      created_at: new Date().toISOString()
    });
    localEvents = localEvents.slice(0, 4);
    renderEventLog();
  }

  function fetchState() {
    return api("/api/state")
      .then(function (nextState) {
        applyState(nextState);
        addLocalEvent("初始运行快照已加载", "success");
      })
      .catch(function (error) {
        setConnection("error", "加载失败");
        showError(error.message);
      });
  }

  function connectEvents() {
    if (eventSource) {
      eventSource.close();
    }
    eventSource = new EventSource("/api/events");
    eventSource.addEventListener("open", function () {
      streamConnected = true;
      setConnection("ok", "实时连接");
      showError("");
      addLocalEvent("实时事件流已连接", "success");
    });
    eventSource.addEventListener("state", function (event) {
      try {
        applyState(JSON.parse(event.data));
      } catch (error) {
        showError("无法解析实时状态: " + error.message);
      }
    });
    eventSource.addEventListener("error", function () {
      streamConnected = false;
      setConnection("warn", "正在重连");
      showError("实时连接中断。界面保留最近快照，并正在重试。");
      if (!reconnectTimer) {
        reconnectTimer = window.setTimeout(function () {
          reconnectTimer = null;
          fetchState();
        }, 2500);
      }
    });
  }

  function api(path, options) {
    var request = options || {};
    request.headers = Object.assign({ Accept: "application/json" }, request.headers || {});
    if (request.body && !request.headers["Content-Type"]) {
      request.headers["Content-Type"] = "application/json";
    }
    return fetch(path, request).then(function (response) {
      if (response.ok) {
        return response.status === 204 ? null : response.json();
      }
      return response.text().then(function (text) {
        var payload = null;
        try {
          payload = text ? JSON.parse(text) : null;
        } catch (_error) {
          payload = null;
        }
        var message = payload && payload.error ? payload.error : (text || response.statusText);
        var error = new Error(message || (path + " 返回 " + response.status));
        error.status = response.status;
        error.payload = payload;
        throw error;
      });
    });
  }

  function applyState(nextState) {
    state = nextState || {};
    var endpoints = asArray(state.endpoints);
    if (!selectedEndpointId || !findEndpoint(selectedEndpointId)) {
      var preferred = endpoints.find(function (endpoint) {
        return endpoint.phase === "active";
      }) || endpoints.find(function (endpoint) {
        return endpoint.held;
      }) || endpoints[0];
      selectedEndpointId = preferred ? preferred.id : null;
    }
    render();
    if (streamConnected) {
      setConnection("ok", "实时连接");
    }
    showError("");
    markSynced();
    if (selectedEndpointId) {
      fetchInspector(selectedEndpointId);
    } else {
      renderInspectorEmpty();
    }
  }

  function render() {
    renderHeader();
    renderCapacity();
    renderGraph();
    renderMessages();
    renderSelectedContext();
    renderEventLog();
    renderBusyState();
  }

  function renderHeader() {
    els.revisionText.textContent = "graph rev " + value(state.graph_revision);
  }

  function renderCapacity() {
    var capacity = state.capacity || {};
    els.desiredCapacity.textContent = value(capacity.desired);
    els.metricDesired.textContent = value(capacity.desired);
    els.metricHealthy.textContent = value(capacity.healthy);
    els.metricActive.textContent = value(capacity.active);
    els.metricWaiting.textContent = value(capacity.waiting);
    els.capacityRevision.textContent = "capacity rev " + value(capacity.revision);
    els.decreaseCapacity.disabled = capacityBusy || Number(capacity.desired || 0) <= 0;
    els.increaseCapacity.disabled = capacityBusy || Number(capacity.desired || 0) >= Number(capacity.healthy || 0);
  }

  function renderGraph() {
    var endpoints = asArray(state.endpoints);
    var tasks = asArray(state.tasks);
    var byTask = {};

    endpoints.forEach(function (endpoint) {
      var taskId = value(endpoint.task_id || endpoint.task || "unassigned");
      if (!byTask[taskId]) {
        byTask[taskId] = [];
      }
      byTask[taskId].push(endpoint);
    });

    els.graphSummary.textContent = endpoints.length + " endpoints / " + asArray(state.edges).length + " edges";
    els.graphList.replaceChildren();

    if (!endpoints.length) {
      els.graphList.appendChild(emptyState("协调图为空", "通过 Manager 创建一个可运行 endpoint。"));
      return;
    }

    var header = element("div", "graph-header");
    header.setAttribute("aria-hidden", "true");
    header.appendChild(element("span", "graph-header__task", "Task"));
    PHASES.forEach(function (phase) {
      header.appendChild(element("span", "graph-header__phase", PHASE_LABELS[phase]));
    });
    els.graphList.appendChild(header);

    tasks.forEach(function (task) {
      var taskId = value(task.id || task.task_id || task.name);
      var taskEndpoints = byTask[taskId] || [];
      if (!taskEndpoints.length) {
        return;
      }
      var lane = element("article", "graph-lane");
      lane.setAttribute("aria-label", "Task " + taskId);

      var identity = element("div", "task-identity");
      identity.appendChild(element("strong", "task-title", task.title || task.name || taskId));
      identity.appendChild(element("code", "task-id", taskId));
      lane.appendChild(identity);

      PHASES.forEach(function (phase) {
        var slot = element("div", "phase-slot phase-slot--" + phase);
        var matches = taskEndpoints.filter(function (endpoint) {
          return normalizePhase(endpoint) === phase;
        });
        if (!matches.length) {
          slot.classList.add("is-empty");
          slot.appendChild(element("span", "slot-placeholder", ""));
        } else {
          matches.forEach(function (endpoint) {
            slot.appendChild(endpointButton(endpoint));
          });
        }
        lane.appendChild(slot);
      });
      els.graphList.appendChild(lane);
    });
  }

  function endpointButton(endpoint) {
    var id = endpoint.id || endpoint.endpoint_id || endpoint.name;
    var descriptor = endpointState(endpoint);
    var button = element("button", "endpoint-node state-" + descriptor.key);
    button.type = "button";
    button.dataset.endpointId = id;
    button.setAttribute("aria-pressed", String(id === selectedEndpointId));
    button.setAttribute("aria-label", (endpoint.title || id) + "，" + descriptor.label + "，查看 endpoint 详情");
    button.addEventListener("click", function () {
      selectedEndpointId = id;
      selectedInspector = null;
      memoryScope = "current";
      renderGraph();
      renderSelectedContext();
      fetchInspector(id, true);
    });

    var top = element("span", "endpoint-node__top");
    var identity = element("span", "endpoint-node__identity");
    identity.appendChild(icon(descriptor.icon));
    identity.appendChild(element("code", "endpoint-node__id", id));
    top.appendChild(identity);
    top.appendChild(element("span", "node-status node-status--" + descriptor.key, descriptor.label));

    button.appendChild(top);
    button.appendChild(element("strong", "endpoint-node__title", endpoint.title || id));

    var meta = element("span", "endpoint-node__meta");
    meta.appendChild(dataPair("gen", value(endpoint.generation)));
    meta.appendChild(dataPair("lease", value(endpoint.lease_id)));
    if (endpoint.checkpoint) {
      meta.appendChild(dataPair("checkpoint", endpoint.checkpoint));
    }
    button.appendChild(meta);

    var dependencies = asArray(endpoint.dependencies).map(function (item) {
      return value(item.value || item.id || item);
    });
    button.appendChild(element(
      "span",
      "endpoint-node__deps",
      dependencies.length ? "依赖 " + dependencies.join(", ") : "入口节点"
    ));
    if (endpoint.last_decision_ref) {
      button.appendChild(element("code", "endpoint-node__decision", "decision " + endpoint.last_decision_ref));
    }
    return button;
  }

  function renderMessages() {
    var manager = state.manager || {};
    var messages = asArray(manager.messages);
    var decisions = asArray(manager.decisions);
    els.managerMessages.replaceChildren();

    if (!messages.length) {
      els.managerMessages.appendChild(emptyState("还没有 Manager 决策", "选择图节点后发送暂停、恢复、完成或图调整指令。"));
      return;
    }

    messages.slice().reverse().slice(0, 6).forEach(function (message) {
      var decision = decisions.find(function (item) {
        return item.manager_input_ref === message.input_ref;
      }) || {};
      var item = element("article", "message-row");
      var head = element("div", "message-row__head");
      head.appendChild(element("code", "message-row__target", value(message.endpoint_id)));
      head.appendChild(element("time", "message-row__time", formatTime(message.created_at)));
      item.appendChild(head);
      item.appendChild(element("p", "message-row__body", message.text || message.message || ""));

      var refs = element("dl", "message-row__meta ref-list");
      refs.appendChild(refPair("ManagerInputRef", message.input_ref));
      refs.appendChild(refPair("DecisionRef", decision.decision_ref));
      refs.appendChild(refPair("intent", decision.intent));
      item.appendChild(refs);
      els.managerMessages.appendChild(item);
    });
  }

  function renderSelectedContext() {
    var endpoint = findEndpoint(selectedEndpointId);
    els.selectedEndpointBadge.textContent = endpoint ? "目标 " + endpoint.id : "未选择 endpoint";
    els.selectedEndpointContext.replaceChildren();
    if (!endpoint) {
      els.selectedEndpointContext.appendChild(element("p", "empty-state__body", "先在协调图中选择一个 endpoint。"));
      return;
    }
    var descriptor = endpointState(endpoint);
    els.selectedEndpointContext.appendChild(element("strong", "selected-target__title", endpoint.title || endpoint.id));
    [
      "task " + value(endpoint.task_id),
      descriptor.label,
      "generation " + value(endpoint.generation),
      "lease " + value(endpoint.lease_id),
      "checkpoint " + value(endpoint.checkpoint)
    ].forEach(function (text) {
      els.selectedEndpointContext.appendChild(element("span", "target-token", text));
    });
  }

  function fetchInspector(id, showLoading) {
    var requestId = ++inspectorRequest;
    if (showLoading || !selectedInspector || selectedInspector.endpoint_id !== id) {
      renderInspectorLoading(id);
    }
    return api("/api/endpoints/" + encodeURIComponent(id) + "/inspector")
      .then(function (inspector) {
        if (requestId !== inspectorRequest || id !== selectedEndpointId) {
          return;
        }
        selectedInspector = inspector || {};
        renderInspector(id, selectedInspector);
      })
      .catch(function (error) {
        if (requestId !== inspectorRequest) {
          return;
        }
        showError(error.message);
        els.inspectorEndpoint.textContent = "加载失败";
        [els.inspectorMeta, els.subscriptionsList, els.effectiveUnionList, els.contextSlice, els.memoryBuffer].forEach(function (container) {
          container.setAttribute("aria-busy", "false");
        });
      });
  }

  function renderInspectorLoading(id) {
    els.inspectorEndpoint.textContent = "正在加载 " + id;
    [els.inspectorMeta, els.subscriptionsList, els.contextSlice, els.memoryBuffer].forEach(function (container) {
      container.setAttribute("aria-busy", "true");
      container.replaceChildren(skeletonRow(), skeletonRow());
    });
    els.effectiveUnionList.setAttribute("aria-busy", "true");
    els.effectiveUnionList.replaceChildren(skeletonRow());
  }

  function renderInspectorEmpty() {
    els.inspectorEndpoint.textContent = "未选择 endpoint";
    els.inspectorMeta.replaceChildren();
    els.subscriptionsList.replaceChildren(emptyState("没有订阅视图", "先选择一个协调图节点。"));
    els.effectiveUnionList.replaceChildren();
    els.contextSlice.replaceChildren(emptyState("没有上下文切片", "先选择一个协调图节点。"));
    els.memoryBuffer.replaceChildren(emptyState("没有上下文缓冲区", "先选择一个协调图节点。"));
  }

  function renderInspector(id, inspector) {
    var recent = asArray(inspector.recent);
    var invocation = inspector.current || recent[0] || {};
    els.inspectorEndpoint.textContent = id;

    els.inspectorMeta.replaceChildren();
    [
      ["invocation", value(invocation.id)],
      ["holding", inspector.current ? "active agent" : "当前无 Agent 持有"],
      ["phase", value(invocation.phase)],
      ["generation", value(invocation.generation)],
      ["lease", value(invocation.lease_id)],
      ["history", recent.length + " records"]
    ].forEach(function (entry) {
      var item = element("div", "meta-item");
      item.appendChild(element("span", "meta-label", entry[0]));
      item.appendChild(element("code", "meta-value", entry[1]));
      els.inspectorMeta.appendChild(item);
    });
    els.inspectorMeta.setAttribute("aria-busy", "false");

    renderList(els.subscriptionsList, asArray(inspector.subscriptions), function (subscription) {
      return {
        title: value(subscription.kind) + " to " + value(subscription.target_id),
        meta: value(subscription.id),
        status: subscription.active ? "active 当前" : "historical 历史",
        statusClass: subscription.active ? "active" : "historical"
      };
    }, "没有订阅子图", "运行或恢复 endpoint 后会创建 generation 级订阅。");

    els.effectiveUnionList.replaceChildren();
    var union = asArray(inspector.effective_subgraph_union).map(function (item) {
      return value(item.value || item.id || item);
    });
    if (!union.length) {
      els.effectiveUnionList.appendChild(element("span", "empty-inline", "有效并集为空"));
    } else {
      union.forEach(function (endpointId) {
        els.effectiveUnionList.appendChild(element("code", "union-chip", endpointId));
      });
    }
    els.effectiveUnionList.setAttribute("aria-busy", "false");

    renderList(els.contextSlice, asArray(inspector.context_slice), function (entry) {
      return {
        title: value(entry.source_endpoint_id),
        meta: value(entry.kind),
        body: entry.text || ""
      };
    }, "当前 Context Slice 为空", "上下文只来自当前 generation 的有效订阅并集。");

    renderMemoryBuffer();
  }

  function renderMemoryBuffer() {
    if (!selectedInspector) {
      return;
    }
    els.memoryBuffer.replaceChildren();
    var invocation = selectedInspector.current || asArray(selectedInspector.recent)[0] || {};
    var buffer = asArray(selectedInspector.task_memory_buffer).map(function (item) {
      return value(item.value || item.id || item);
    });
    var candidates = asArray(selectedInspector.candidates);

    if (memoryScope === "current") {
      candidates = invocation.id ? candidates.filter(function (candidate) {
        return candidate.created_by_invocation_id === invocation.id;
      }) : [];
    }

    var owner = element("div", "buffer-owner");
    owner.appendChild(element("span", "list-kicker", "TaskMemoryBuffer"));
    if (!buffer.length) {
      owner.appendChild(element("span", "empty-inline", "没有 owner 字段"));
    } else {
      var tokens = element("div", "buffer-token-list");
      buffer.forEach(function (entry) {
        tokens.appendChild(element("code", "buffer-token", entry));
      });
      owner.appendChild(tokens);
    }
    els.memoryBuffer.appendChild(owner);

    if (!candidates.length) {
      els.memoryBuffer.appendChild(emptyState(
        memoryScope === "current" ? "当前 invocation 没有候选上下文" : "当前 Task 没有候选上下文",
        "切换范围或等待运行时创建候选。"
      ));
    } else {
      candidates.forEach(function (candidate) {
        var item = element("article", "list-row");
        var head = element("div", "list-row__head");
        head.appendChild(element("strong", "list-row__title", value(candidate.endpoint_id)));
        head.appendChild(element(
          "span",
          "state-tag " + (candidate.prerequisites_satisfied ? "state-tag--success" : "state-tag--warning"),
          candidate.prerequisites_satisfied ? "前置满足" : "等待前置"
        ));
        item.appendChild(head);
        item.appendChild(element("p", "list-row__body", candidate.reason || "可用于扩展执行上下文"));
        var meta = element("div", "list-row__meta");
        meta.appendChild(element("code", "data-ref", "creator " + value(candidate.created_by_invocation_id)));
        meta.appendChild(element(
          "span",
          "state-tag",
          candidate.would_expand_effective_view ? "将扩展并集" : "已在并集"
        ));
        item.appendChild(meta);
        els.memoryBuffer.appendChild(item);
      });
    }
    els.memoryBuffer.setAttribute("aria-busy", "false");
    els.memoryCurrent.setAttribute("aria-pressed", String(memoryScope === "current"));
    els.memoryTask.setAttribute("aria-pressed", String(memoryScope === "task"));
  }

  function setMemoryScope(scope) {
    memoryScope = scope;
    renderMemoryBuffer();
  }

  function renderList(container, items, mapItem, emptyTitle, emptyBody) {
    container.replaceChildren();
    container.setAttribute("aria-busy", "false");
    if (!items.length) {
      container.appendChild(emptyState(emptyTitle, emptyBody));
      return;
    }
    items.forEach(function (item) {
      var view = mapItem(item);
      var node = element("article", "list-row");
      var head = element("div", "list-row__head");
      head.appendChild(element("strong", "list-row__title", view.title));
      if (view.status) {
        head.appendChild(element("span", "state-tag state-tag--" + view.statusClass, view.status));
      }
      node.appendChild(head);
      if (view.meta) {
        node.appendChild(element("code", "list-row__meta data-ref", view.meta));
      }
      if (view.body) {
        node.appendChild(element("p", "list-row__body", String(view.body)));
      }
      container.appendChild(node);
    });
  }

  function renderEventLog() {
    if (!state) {
      return;
    }
    var managerEvents = asArray(state.manager && state.manager.events).slice().reverse().map(function (event) {
      return {
        text: event.text || event.type || "Manager event",
        type: event.type || "event",
        endpoint_id: event.endpoint_id,
        input_ref: event.input_ref,
        decision_ref: event.decision_ref,
        created_at: event.created_at,
        kind: event.type && event.type.indexOf("hold") >= 0 ? "warning" : "info"
      };
    });
    var entries = localEvents.concat(managerEvents).slice(0, 10);
    els.eventList.replaceChildren();
    if (!entries.length) {
      var emptyItem = element("li", "event-row");
      emptyItem.appendChild(emptyState("还没有运行事件", "执行并发或 Manager 操作后会在这里留下证据。"));
      els.eventList.appendChild(emptyItem);
      return;
    }
    entries.forEach(function (entry) {
      var item = element("li", "event-row event-row--" + (entry.kind || "info"));
      item.appendChild(element("time", "event-row__time", formatTime(entry.created_at)));
      var content = element("div", "event-row__content");
      content.appendChild(element("strong", "event-row__type", entry.type || "local"));
      content.appendChild(element("p", "event-row__body", entry.text));
      var refs = [entry.endpoint_id, entry.input_ref, entry.decision_ref].filter(Boolean);
      if (refs.length) {
        content.appendChild(element("code", "event-row__meta", refs.join(" / ")));
      }
      item.appendChild(content);
      els.eventList.appendChild(item);
    });
  }

  function sendManagerMessage(event) {
    event.preventDefault();
    sendManagerText(els.managerInput.value.trim(), true);
  }

  function sendManagerText(text, clearInput) {
    if (managerBusy) {
      return;
    }
    if (!text) {
      showError("请输入一条 Manager 指令。");
      els.managerInput.focus();
      return;
    }
    if (!selectedEndpointId) {
      showError("请先在协调图中选择 endpoint。");
      return;
    }
    managerBusy = true;
    renderBusyState();
    showError("");
    api("/api/manager/messages", {
      method: "POST",
      body: JSON.stringify({
        text: text,
        endpoint_id: selectedEndpointId,
        expected_revision: state && state.graph_revision
      })
    }).then(function (result) {
      if (clearInput) {
        els.managerInput.value = "";
      }
      addLocalEvent("Manager 已记录 " + value(result && result.decision_ref), "success");
      if (result && result.state) {
        applyState(result.state);
        return null;
      }
      return fetchState();
    }).catch(function (error) {
      recoverConflict(error);
      showError("Manager 指令未应用: " + error.message);
    }).finally(function () {
      managerBusy = false;
      renderBusyState();
    });
  }

  function changeCapacity(delta) {
    if (capacityBusy) {
      return;
    }
    var capacity = state && state.capacity ? state.capacity : {};
    var desired = Math.max(0, Number(capacity.desired || 0) + delta);
    capacityBusy = true;
    renderBusyState();
    showError("");
    api("/api/capacity", {
      method: "POST",
      body: JSON.stringify({
        desired: desired,
        expected_revision: capacity.revision
      })
    }).then(function (nextState) {
      addLocalEvent("期望并发已调整为 " + desired + "，active invocation 自然排空", "success");
      applyState(nextState);
    }).catch(function (error) {
      recoverConflict(error);
      showError("并发调整未应用: " + error.message);
    }).finally(function () {
      capacityBusy = false;
      renderBusyState();
    });
  }

  function recoverConflict(error) {
    if (error && error.payload && error.payload.state) {
      applyState(error.payload.state);
      addLocalEvent("检测到 revision 冲突，已刷新到服务器快照", "warning");
    }
  }

  function renderBusyState() {
    var capacity = state && state.capacity ? state.capacity : {};
    els.decreaseCapacity.disabled = capacityBusy || Number(capacity.desired || 0) <= 0;
    els.increaseCapacity.disabled = capacityBusy || Number(capacity.desired || 0) >= Number(capacity.healthy || 0);
    els.decreaseCapacity.closest(".capacity-controls").classList.toggle("is-busy", capacityBusy);
    els.messageForm.classList.toggle("is-busy", managerBusy);
    els.messageForm.setAttribute("aria-busy", String(managerBusy));
    els.managerSubmit.disabled = managerBusy;
    document.querySelectorAll("[data-prompt]").forEach(function (button) {
      button.disabled = managerBusy;
    });
  }

  function findEndpoint(id) {
    return asArray(state && state.endpoints).find(function (endpoint) {
      return (endpoint.id || endpoint.endpoint_id || endpoint.name) === id;
    });
  }

  function endpointState(endpoint) {
    var phase = String(endpoint.phase || endpoint.status || endpoint.state || "").toLowerCase();
    if (endpoint.satisfied || phase === "satisfied" || phase === "completed") {
      return { key: "completed", label: "已完成", icon: "check" };
    }
    if (endpoint.held || phase === "held") {
      return { key: "held", label: "已暂停，可恢复", icon: "pause" };
    }
    if (phase === "active" || phase === "running" || phase === "execute") {
      return { key: "active", label: "执行中", icon: "activity-heartbeat" };
    }
    return { key: "waiting", label: "等待", icon: "git-branch" };
  }

  function normalizePhase(endpoint) {
    var descriptor = endpointState(endpoint);
    if (descriptor.key === "completed") {
      return "verify";
    }
    if (descriptor.key === "active" || descriptor.key === "held") {
      return "execute";
    }
    return "plan";
  }

  function dataPair(label, data) {
    var pair = element("span", "data-pair");
    pair.appendChild(element("span", "data-pair__label", label));
    pair.appendChild(element("code", "data-pair__value", data));
    return pair;
  }

  function refPair(label, data) {
    var fragment = document.createDocumentFragment();
    fragment.appendChild(element("dt", "ref-list__label", label));
    fragment.appendChild(element("dd", "ref-list__value", value(data)));
    return fragment;
  }

  function icon(name) {
    var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    var use = document.createElementNS("http://www.w3.org/2000/svg", "use");
    svg.setAttribute("class", "icon");
    svg.setAttribute("aria-hidden", "true");
    svg.setAttribute("focusable", "false");
    use.setAttribute("href", "#icon-" + name);
    svg.appendChild(use);
    return svg;
  }

  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) {
      node.className = className;
    }
    if (text !== undefined && text !== null) {
      node.textContent = String(text);
    }
    return node;
  }

  function emptyState(title, body) {
    var node = element("div", "empty-state");
    node.appendChild(element("strong", "empty-state__title", title));
    if (body) {
      node.appendChild(element("p", "empty-state__body", body));
    }
    return node;
  }

  function skeletonRow() {
    var node = element("div", "skeleton-row");
    node.setAttribute("aria-hidden", "true");
    node.appendChild(element("span", "skeleton-row__line skeleton-row__line--strong", ""));
    node.appendChild(element("span", "skeleton-row__line", ""));
    return node;
  }

  function asArray(input) {
    if (!input) {
      return [];
    }
    if (Array.isArray(input)) {
      return input;
    }
    if (typeof input !== "object") {
      return [input];
    }
    return Object.keys(input).map(function (key) {
      var item = input[key];
      if (item && typeof item === "object" && !Array.isArray(item)) {
        return Object.assign({ id: item.id || key }, item);
      }
      return { id: key, value: item };
    });
  }

  function value(input) {
    if (input === undefined || input === null || input === "") {
      return "-";
    }
    return String(input);
  }

  function formatTime(input) {
    if (!input) {
      return "刚刚";
    }
    var date = new Date(input);
    if (Number.isNaN(date.getTime())) {
      return "刚刚";
    }
    return date.toLocaleTimeString("zh-CN", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }
}());
