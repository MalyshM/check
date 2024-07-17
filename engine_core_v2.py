# workflow - engine core v2
# ref - cc_wf:6038b966166c12000f515a2f

# disable marked messages from other service
def disabled_service_integration(question):
    if re.search("пишу из приложения 2гис.", question.lower()):
        target = "пишу из приложения 2гис."
        question = re.sub(target, "", question.lower())
    return question


# region INIT VARS
ENGINE_VERSION = "2.8.3"

debug_obj = {}
question = disabled_service_integration(question) or ""
session_guid = session_guid or dialog_id or "EMPTY_SESSION_GUID"
dialog_id = session_guid  # FIXME: deprecated
last_state = {} if last_state is None else last_state

# states
dialog_state = nonedict(last_state.get("dialog_state", {}))
node_state = nonedict(last_state.get("node_state", {}))
engine_state = nonedict(last_state.get("engine_state", {}))
global_state = nonedict(last_state.get("global_state", {}))

# vars
MAX_COUNT_TRAICBACK = MAX_COUNT_TRAICBACK or 33
engine_context = engine_context or {}
node_traiceback = []
debug_obj["node_traiceback"] = node_traiceback

_is_break = False
attachments = attachments or []
is_sip = source == 'sip'
sip_agi = sip_agi or {}
phone_number = sip_agi.get("calleridname", "")

working_time = sip_agi.get("IS_WORKING_TIME", "")
is_working_time = not (working_time == "0")
call_id = sip_agi.get("CALL_ID", "")
full_name = sip_agi.get("FULL_NAME", "")
pin = sip_agi.get("PIN", "")
office_id = sip_agi.get("OFFICE_ID", "")
phone_callback = sip_agi.get("PHONE", "")
visit_date = sip_agi.get("VISIT_DATE", "")
re_number = sip_agi.get("RE_NUMBER", "")

is_dev = bool(is_dev)  # default False
messages = []
events = []
commands = []
nets = {}

# ==== GET IN ENGINE CONFIG ====
map_min_prob = {}
map_nodes = {}
map_reaction = {}
settings = {}
nets_wf = {}
start_node = None
ccmap = None
preprocess_code = None
postprocess_code = None
engine_conf_name = None

# endregion

# region CONTEXT FUNCS


def intent(net_name, class_id=None, min_prob=None, min_prob_class=0):
    answer_id = nets.get(f"{net_name}_answer_id", None)
    answer_prob = nets.get(f"{net_name}_answer_prob", None)

    if answer_id is None or answer_prob is None:
        raise Exception(f'Nets "{net_name}" not found in intent function')

    if min_prob is None:
        min_prob = map_min_prob.get(net_name, map_min_prob["nets"])

    if float(answer_prob) < float(min_prob):
        answer_id = min_prob_class

    if class_id is None:
        return str(answer_id)
    elif isinstance(class_id, list):
        return str(answer_id) in set([str(x) for x in class_id])
    else:
        return str(answer_id) == str(class_id)


def add_command(code: str, data: dict, sub_guids: list = None):
    global commands
    commands.append({
        "code": code,
        "data": data,
        "meta": {
            "session_guid": session_guid,
            "question": question,
            "sub_guids": sub_guids or []
        }
    })


def add_event(event_name, event_start, event_timeout=None, event_data=None):
    """
        event_start:
            - after_play
            - after_timeout
            - now
    """
    global events

    if event_name in {x["name"] for x in events}:
        return

    events.append({
        "name": event_name,
        "start": event_start,
        "timeout": event_timeout,
        "data": event_data
    })


def reaction_self(reaction_name, reaction_self="nofaq"):

    reaction_ref = map_reaction.get(reaction_name, None)

    if reaction_ref is not None:

        return reaction_name

    return reaction_self


def reaction(name, build_state={}, no_add: bool = False, **params):
    if isinstance(name, list):
        for n in name:
            reaction(n, build_state, **params)
        return

    global messages

    reaction_ref = map_reaction.get(name, None)

    if reaction_ref is None:
        if name == "error":
            reaction_ref = Ref("ds_reactions:5f4d3d95e982ac000ee5ac9b")
        else:
            raise Exception(f'reaction "{name}" not found')

    data = run_wf(
        {
            "reaction": reaction_ref,
            "build_state": build_state,
            "msg_id": name
        },
        _make_reaction_ref
    )
    message = data["message"]

    if params:
        message.update(params)

    if message.get("is_redirect", False):
        add_event("redirect", "after_play")
    elif message.get("is_exit", False):
        add_event("exit", "after_play")
    elif message.get("redirect_timeout", 0) > 0:
        add_event("redirect", "after_timeout",
                  event_timeout=message["redirect_timeout"])
    elif message.get("exit_timeout", 0) > 0:
        add_event("exit", "after_timeout",
                  event_timeout=message["exit_timeout"])

    if not no_add:
        messages.append(message)
    return message


def reaction_callback(name, build_state={}, **params):
    if not is_sip:
        return
    msg = reaction(name, build_state, no_add=True, **params)
    run_cf(
        "codeflows:62e0e30dd0bcf7000f05d923",
        {
            "session_guid": session_guid,
            "message": msg
        }
    )


def worktime_reaction(name, build_state={}, **params):
    return reaction(
        name,
        build_state={
            "is_working_time": is_working_time,
            "is_sip": is_sip,
            **build_state
        },
        **{
            "is_redirect": is_working_time,
            **params
        }
    )


def clear_reactions():
    global messages
    messages.clear()


def set_break(flag=True):
    global _is_break
    _is_break = flag


def is_break():
    return _is_break


def set_node(node_name=None, **settings):
    global node_state

    if node_name is None:
        node = start_node
        node_name = "START_NODE"
    else:
        node = map_nodes.get(node_name, None)
        if node is None:
            raise Exception(f'node "{node_name}" not found')

    engine_state["node_name"] = node_name
    if node:
        engine_state["node"] = node
        engine_state["node_ver"] = node.ver
    else:
        engine_state["node"] = None
        engine_state["node_ver"] = None

    node_state = nonedict({**settings})


def init_dialog_default(_state):
    # TODO: make in engine_confe field with dialog_state_defaults as pyconf
    for key, val in _state.items():
        if key not in dialog_state:
            dialog_state[key] = val

# endregion

# region API


class ExecContext:
    compile_cache = {}

    def __init__(self, ctx) -> None:
        self.ctx = {**ctx}

    def compile(self, name: str, code: str, code_hash: str = None):
        code_hash = hashlib.md5(code.encode()).hexdigest()
        code_obj = self.compile_cache.get(code_hash)
        if code_obj is None:
            code_obj = compile(code, name, 'exec')
            self.compile_cache[code_hash] = code_obj
        return code_obj

    def exec(self, name, code):
        try:
            code_obj = self.compile(name, code)
        except Exception:
            raise Exception(f'Error compile "{name}" code:\n{traceback_msg()}')

        try:
            exec(code_obj, self.ctx, self.ctx)
        except Exception:
            raise Exception(f'Error exec "{name}" code:\n{traceback_msg()}')

    def __getitem__(self, key: str):
        return self.ctx.get(key, None)


def make_exec_context() -> ExecContext:
    return ExecContext({
        "debug_obj": debug_obj,
        "dialog_state": dialog_state,
        "node_state": node_state,
        "engine_state": engine_state,
        "global_state": global_state,
        "source": source,
        "sip_agi": sip_agi,
        "is_sip": is_sip,
        "phone_number": phone_number,
        # Убрать после перевода на is_working_time во всех проектах
        "working_time": working_time,
        "is_working_time": is_working_time,
        "call_id": call_id,
        "full_name": full_name,
        "pin": pin,
        "visit_date": visit_date,
        "office_id": office_id,
        "phone_callback": phone_callback,
        "is_dev": is_dev,
        "messages": messages,
        "jwt": None,
        "nets": nets,
        "attachments": attachments,
        # ==== funcs =====
        "intent": intent,
        "add_event": add_event,
        "add_command": add_command,
        "reaction": reaction,
        "reaction_self": reaction_self,
        "reaction_callback": reaction_callback,
        "worktime_reaction": worktime_reaction,
        "clear_reactions": clear_reactions,
        "set_break": set_break,
        "is_break": is_break,
        "set_node": set_node,
        "init_dialog_default": init_dialog_default,
        # ==== params ====
        "ccmap": ccmap,  # comand classess map
        "last_state": last_state,
        "question": question,
        "engine_context": engine_context,
        "settings": settings,
        "dialog_id": dialog_id,
        "session_guid": session_guid,
        "owner_ref": owner_ref or '123123',
    })


class ExecObject:
    def __init__(self, code_name: str, code: str):
        self.code = code
        self.code_name = code_name

    def _exec_deps(self, ctx: ExecContext):
        raise NotImplementedError()

    def exec(self, ctx: ExecContext):
        self._exec_deps(ctx)
        ctx.exec(self.code_name, self.code)


class PyCode(ExecObject):
    def __init__(self, data):
        super().__init__(
            f'pycode<{data["ref"]}>"{data["name"]}"', data["code"])
        self.code = data["code"]
        self.deps = data["deps"]
        self.name = data["name"]

    def _exec_deps(self, ctx: ExecContext):
        for dep in self.deps:
            dep.exec(ctx)

    def run(self):
        ctx = make_exec_context()
        self.exec(ctx)


class Node(ExecObject):
    def __init__(self, data) -> None:
        super().__init__(f'node<{data["ref"]}>"{data["name"]}"', data["code"])
        self.code = data["code"]
        self.defs = data["defs"]
        self.deps = data["deps"]
        self.is_release = data["is_release"]
        self.name = data["name"]
        self.ref = data["ref"]
        self.ver = data["ver"]
        self.no_preprocessing = data.get("no_preprocessing", False)
        self.no_postprocessing = data.get("no_postprocessing", False)

    def _exec_deps(self, ctx: ExecContext):
        for dep in self.deps:
            dep.exec(ctx)

    def get_meta_json(self):
        global node_state
        return {
            "name": self.name,
            "ref": str(self.ref),
            "is_release": self.is_release,
            "ver": self.ver,
            "state": copy.deepcopy(dict(node_state))
        }

    def init_state(self):
        global node_state
        for key, val in self.defs.items():
            if key not in node_state:
                node_state[key] = val

    def run(self):
        ctx = make_exec_context()
        self.exec(ctx)
        if ctx["node_func"]:
            ctx["node_func"]()

    def run_preprocessing(self):
        global preprocess_code
        if not self.no_preprocessing and preprocess_code:
            preprocess_code.run()

    def run_postprocessing(self):
        global postprocess_code
        if not self.no_postprocessing and postprocess_code:
            postprocess_code.run()


def run_cf(cf_ref, state: dict):
    app_ref = __meta__["app_ref"]
    ma_session = __meta__["ma_session"]
    rsp = app_requests.post(
        'cf',
        'run_cf',
        json={
            "cf_ref": Ref(cf_ref).get_json(),
            "app_ref": Ref(app_ref).get_json(),
            "state": json.loads(json_serialize(state))
        },
        cookies={"ma_session": ma_session},
        timeout=60
    )
    if rsp.status_code != 200:
        raise Exception(
            f'not valid response for run_cf(code={rsp.status_code}):\n{rsp.text}')
    return conver_to_ref(rsp.json())


def run_wf(state, wf_ref):
    rsp = app_requests.post(
        "wf",
        "run_wf",
        json={
            "state": json.loads(json_serialize(state)),
            "wf_ref": wf_ref.get_json(),
            "app_ref": __meta__["app_ref"].get_json()
        },
        timeout=10
    )

    if rsp.status_code != 200:
        raise Exception(
            f'Not valid response \nstatus: {rsp.status_code} \ndata:\n{rsp.text}'
        )

    return conver_to_ref(rsp.json())


def get_from_link(obj_ref, fields: list):
    rsp = app_requests.post(
        "wf",
        "getFromLink",
        json={
            "ref": obj_ref.get_json(),
            "_var_in": [
                "ref"
            ] + fields,
            "_var_out": fields
        }
    )
    if rsp.status_code != 200:
        raise Exception(
            f"nat valid response(status_code={rsp.status_code}):\n{rsp.text}")
    return rsp.json()


def make_map_min_prob():
    global map_min_prob

    for key, value in settings.items():
        key_match = re.search(r'^(.+)_min_prob$', key)
        if key_match:
            map_min_prob[key_match.group(1)] = float(value)

    map_min_prob["nets"] = map_min_prob.get("nets", 0.65)


def make_pycode(data):
    if data is None:
        return None
    data["deps"] = data["deps"] or []
    data["deps"] = [make_pycode(x) for x in data["deps"]]
    return PyCode(data)


def make_node(data):
    data["defs"] = data["defs"] or {}
    data["defs"] = {
        x["key"]: eval(x["value"])
        for x in data["defs"]
    }
    data["deps"] = data["deps"] or []
    data["deps"] = [make_pycode(x) for x in data["deps"]]
    return Node(data)


def build_engine_config():
    global map_reaction
    global map_nodes
    global nets_wf
    global start_node
    global preprocess_code
    global postprocess_code
    global settings
    global engine_conf_name
    global ccmap

    data = run_cf(
        _cf_build_ec,
        {
            "engine_config": engine_config
        }
    )
    debug_obj["engine_from_cache"] = data["from_cache"]
    map_reaction = data["map_reaction"]
    nets_wf = data["nets_wf"]

    if data["ccmap"]:
        ccmap = {
            key: {
                "excluded_nodes": val["excluded_nodes"],
                "name": val["name"],
                "code": make_pycode(val["code"])
            }
            for key, val in data["ccmap"]["map"].items()
        }

    start_node = make_node(data["start_node"])
    map_nodes = {
        key: make_node(node_data)
        for key, node_data in data["map_nodes"].items()
    }
    map_nodes["START_NODE"] = start_node
    preprocess_code = make_pycode(
        data["preprocess_code"]) if data["preprocess_code"] else None
    postprocess_code = make_pycode(
        data["postprocess_code"]) if data["postprocess_code"] else None

    settings = nonedict(data["settings"])
    engine_conf_name = data["engine_conf_name"]


def init_states():
    global dialog_state
    global node_state
    global engine_state

    # set_node
    is_reset = False
    cause_reset = None

    if engine_state["node_name"] and engine_state["node_ver"]:
        node = map_nodes.get(engine_state["node_name"], None)
        if node is None:
            cause_reset = f'current node name "{engine_state["node_name"]}" not in map_nodes'
            is_reset = True
        elif node.ver != engine_state["node_ver"]:
            cause_reset = f'current node version "{node.ver}"" not equal select version "{engine_state["node_ver"]}"'
            is_reset = True
        else:
            engine_state["node"] = node
            engine_state["node_ver"] = node.ver
    else:
        cause_reset = f'node_name or node_ver not defined'
        is_reset = True

    if engine_state["version"] != ENGINE_VERSION:
        cause_reset = f'current engine version "{ENGINE_VERSION}"" not equal select version "{engine_state["version"]}"'
        is_reset = True

    if is_reset:
        dialog_state.clear()
        node_state.clear()
        engine_state.clear()
        set_node()
    engine_state["count_question"] = engine_state["count_question"] + \
        1 if engine_state["count_question"] is not None else 1
    engine_state["engine_conf_name"] = engine_conf_name
    engine_state["make_reset_states"] = is_reset
    engine_state["cause_reset"] = cause_reset

    engine_state["last_run_time"] = engine_state["run_time"]
    engine_state["run_time"] = time.time()

    engine_state["run_time_delta"] = (
        engine_state["run_time"] - engine_state["last_run_time"]) if engine_state["last_run_time"] else 0

    engine_state["version"] = ENGINE_VERSION
    dialog_state["reaction_state"] = None
    dialog_state["reaction_prob"] = None


def make_nets():
    global nets

    if nets_wf:
        nets = run_wf(
            {
                "question": question,
                "attachments": attachments
            },
            nets_wf
        )


def clear_states():
    global dialog_state
    global node_state
    global engine_state

    if dialog_state["reaction_state"] is not None:
        dialog_state["reaction_state"] = str(dialog_state["reaction_state"])
    if dialog_state["reaction_prob"] is not None:
        dialog_state["reaction_prob"] = str(dialog_state["reaction_prob"])
    if "history_reaction_state" not in dialog_state:
        dialog_state["history_reaction_state"] = []
    dialog_state["history_reaction_state"].append(
        dialog_state["reaction_state"]
    )
    dialog_state["history_reaction_state"] = dialog_state["history_reaction_state"][-10:]

    node_state = dict(node_state)
    dialog_state = dict(dialog_state)
    engine_state = dict(engine_state)
    if isinstance(engine_state["node"], Node):
        engine_state["node"] = engine_state["node"].get_meta_json()


def run_logic():
    debug_obj["run_node"] = engine_state["node"].get_meta_json()
    debug_obj["end_node"] = None

    engine_state["node"].run_preprocessing()

    while not _is_break:  # dialog loop
        node = engine_state["node"]  # type: Node
        with tracer.active_span(f'run "{node.name}" node') as scope:
            span = scope.span
            span.set_tag("node", str(node.get_meta_json()))
            try:
                node.init_state()
                node_traiceback.append(node.get_meta_json())

                if len(node_traiceback) > MAX_COUNT_TRAICBACK:
                    raise Exception(f'Max count traicback')

                node.run()
            except Exception as e:
                raise Exception(
                    f'Error in {node.name} node({node.ref}):\n{traceback_msg()}')

    engine_state["node"].run_postprocessing()
    debug_obj["end_node"] = engine_state["node"].get_meta_json()

    # if not messages:
    #     raise Exception('Logic return 0 messages')

# endregion

# region DS RUN LOGIC


try:
    with tracer.active_span('build_engine_config') as scope:
        build_engine_config()

    with tracer.active_span('make_map_min_prob') as scope:
        make_map_min_prob()

    with tracer.active_span('init_states') as scope:
        init_states()

    with tracer.active_span('make_nets') as scope:
        make_nets()

    with tracer.active_span('run_logic') as scope:
        run_logic()
except Exception as e:
    debug_obj["Exception"] = f'Error in meta logic:\n{traceback_msg()}'
    debug_obj["ExceptionType"] = str(type(e))
    worktime_reaction("error")
    dialog_state = nonedict()
    set_node()
finally:
    clear_states()

# endregion
