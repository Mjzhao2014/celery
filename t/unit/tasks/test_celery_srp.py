import os
import pytest
from celery import Celery, Task, ConfigurationManager, TaskRegistryManager, ResultManager, WorkerRunner, SignalManager
from celery.signals import worker_init
from celery.loaders.base import BaseLoader
from celery.backends.cache import CacheBackend
from celery.backends.rpc import RPCBackend
import types
import re
from celery.backends.base import DisabledBackend

# SignalManager
@pytest.fixture
def signal_manager():
    app = Celery("test")
    return SignalManager(app)

def test_builtin_signals_are_registered():
    manager = SignalManager(Celery("test"))

    # All built-in signals that must be registered at initialization
    builtin_signals = [
        "worker_init",
        "worker_ready",
        "worker_shutdown",
        "task_prerun",
        "task_postrun",
        "task_success",
        "task_failure",
        "task_revoked",
        "task_retry",
        "task_unknown",
        "celeryd_after_setup",
        "celeryd_init",
        "celeryd_startup",
        "beat_init",
        "beat_embedded_init",
    ]

    for name in builtin_signals:
        assert manager.has_signal(name), f"Signal '{name}' should be registered"
        assert manager.get_signal(name) is not None, f"Signal '{name}' should be retrievable"

def test_disconnect_builtin_signal(signal_manager):
    called = {}

    def handler(**kwargs):
        called['ok'] = True

    signal_manager.connect(worker_init, handler)
    signal_manager.disconnect(worker_init, handler)
    signal_manager.emit(worker_init)
    assert 'ok' not in called

def test_custom_signal(signal_manager):
    called = {}

    signal = signal_manager.register_signal("my_signal")

    def handler(**kwargs):
        called['value'] = kwargs.get("msg")

    signal_manager.connect(signal, handler)
    signal_manager.emit(signal, msg="hello")
    assert called["value"] == "hello"
    
def test_register_duplicated_signal(signal_manager):
    signal_manager.register_signal("duplicate")
    with pytest.raises(ValueError):
        signal_manager.register_signal("duplicate")

def test_get_and_has_signal(signal_manager):
    assert not signal_manager.has_signal("nosignal")
    new_signal = signal_manager.register_signal("exists")
    assert signal_manager.has_signal("exists")
    assert signal_manager.get_signal("exists") is new_signal
    
def test_get_signal_not_found(signal_manager):
    with pytest.raises(KeyError):
        signal_manager.get_signal("nothing")

def test_remove_signal_by_name(signal_manager):
    signal_manager.register_signal("custom_signal")
    assert signal_manager.has_signal("custom_signal")
    signal_manager.remove_signal("custom_signal")
    assert not signal_manager.has_signal("custom_signal")

def test_remove_signal_by_instance(signal_manager):
    sig = signal_manager.register_signal("custom_signal2")
    assert signal_manager.has_signal("custom_signal2")
    signal_manager.remove_signal(sig)
    assert not signal_manager.has_signal("custom_signal2")

def test_remove_builtin_signal_fails(signal_manager):
    with pytest.raises(KeyError):
        signal_manager.remove_signal(worker_init)

def test_builtin_signal_cannot_be_removed(signal_manager):
    try:
        signal_manager.remove_signal(worker_init)
    except KeyError:
        pass  # expected behavior

    assert signal_manager.has_signal("worker_init")


# ConfigurationManager
@pytest.fixture
def manager():
    app = Celery("test")
    return ConfigurationManager(app)

def test_config_from_object_with_dict(manager):
    config = {"broker_url": "redis://localhost"}
    manager.config_from_object(config)
    assert manager.conf["broker_url"] == "redis://localhost"

def test_config_from_object_with_module_string(manager, tmp_path, monkeypatch):
    module_path = tmp_path / "myconfig.py"
    module_path.write_text("broker_url = 'amqp://localhost'")

    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    os.chdir(tmp_path)

    manager.config_from_object("myconfig")
    assert manager.conf["broker_url"] == "amqp://localhost"
    
def test_config_from_object_with_module_object(manager):
    module_obj = types.ModuleType("my_module")
    module_obj.broker_url = "amqp://module-obj"

    manager.config_from_object(module_obj)

    assert manager.conf["broker_url"] == "amqp://module-obj"

@pytest.mark.parametrize("invalid_obj", [
    "notexistmodule",          
    "os:notexistattr",     
    "builtins:int:attr",    
    object(),                          
    123,                              
    None,                               
])
def test_config_from_object_invalid_module_silent_false(manager, invalid_obj):
    with pytest.raises(ImportError):
        manager.config_from_object(invalid_obj, silent=False)

@pytest.mark.parametrize("invalid_obj", [
    "notexistmodule",      
    "os:notexistattr",           
    "builtins:int:attr",       
    object(),                     
    123,                     
    None,     
])
def test_config_from_object_invalid_module_silent_true(manager, invalid_obj):
    try:
        manager.config_from_object(invalid_obj, silent=True)
    except Exception as e:
        pytest.fail(f"{type(e).__name__} was raised when silent=True: {e}")

def test_read_configuration(monkeypatch, tmp_path):
    config_path = tmp_path / "envconfig.py"
    config_path.write_text("task_serializer = 'json'")

    monkeypatch.setenv("CELERY_CONFIG_MODULE", "envconfig")
    os.chdir(tmp_path)

    app = Celery()
    manager = ConfigurationManager(app)
    config = manager.read_configuration()
    assert config["task_serializer"] == "json"
    
def test_read_configuration_with_custom_env(monkeypatch, tmp_path):
    config_path = tmp_path / "custom_config.py"
    config_path.write_text("broker_url = 'redis://custom-env'")

    monkeypatch.setenv("MY_CUSTOM_ENV", "custom_config")
    os.chdir(tmp_path)

    app = Celery()
    manager = ConfigurationManager(app)
    config = manager.read_configuration(env="MY_CUSTOM_ENV")

    assert config["broker_url"] == "redis://custom-env"

def test_cmdline_config_parser_basic(manager):
    args = [
        "broker_url=redis://localhost:6379/0",
        "result_backend=rpc://",
        "worker_concurrency=8",
        "task_acks_late=True",
        "enable_utc=False",
        "accept_content=['json','msgpack']",
        "imports=myapp.tasks,another.module",
        "worker_prefetch_multiplier=1",
        "some_string=hello world",
        "max_retries=5",
    ]

    result = manager.cmdline_config_parser(args)

    assert result["broker_url"] == "redis://localhost:6379/0"
    assert result["result_backend"] == "rpc://"
    assert result["worker_concurrency"] == 8
    assert result["task_acks_late"] is True
    assert result["enable_utc"] is False
    assert result["accept_content"] == ["json", "msgpack"]
    assert result["imports"] == "myapp.tasks,another.module"
    assert result["worker_prefetch_multiplier"] == 1
    assert result["some_string"] == "hello world"
    assert result["max_retries"] == 5
    
def test_cmdline_config_parser_with_cast(manager):
    args = [
        "worker_concurrency=(int)4",
        "max_load=(float)2.5",
        "task_acks_late=(bool)True",
        "accept_content=(list)['json','yaml']",
        "log_level=(str)INFO",
        "pattern1=(regex)^foo$",
        "pattern2=(regex)bar.*baz",
    ]

    result = manager.cmdline_config_parser(args)

    # Verify int casting
    assert result["worker_concurrency"] == 4

    # Verify float casting
    assert result["max_load"] == 2.5

    # Verify bool casting
    assert result["task_acks_late"] is True

    # Verify list parsing
    assert result["accept_content"] == ["json", "yaml"]

    # Verify str passthrough
    assert result["log_level"] == "INFO"

    # Verify regex pattern1
    pattern1 = result["pattern1"]
    assert isinstance(pattern1, re.Pattern), f"Expected compiled regex, got {type(pattern1)}"
    assert pattern1.pattern == "^foo$"

    # Verify regex pattern2
    pattern2 = result["pattern2"]
    assert isinstance(pattern2, re.Pattern), f"Expected compiled regex, got {type(pattern2)}"
    assert pattern2.pattern == "bar.*baz"

def test_config_applies_to_app_conf():
    app = Celery()
    
    # induce cache
    _ = app.conf
    assert "task_serializer" not in app.conf

    manager = ConfigurationManager(app)
    config_dict = {
        "task_serializer": "json",
        "broker_url": "amqp://localhost"
    }
    manager.config_from_object(config_dict)

    assert app.conf["task_serializer"] == "json"
    assert app.conf["broker_url"] == "amqp://localhost"

def test_configuration_manager_constructor_signature():
    """Test that ConfigurationManager constructor accepts only app as required argument."""
    app = Celery("test")
    
    # Should work with only app
    manager = ConfigurationManager(app)
    
    # Test that required fields are properly initialized by constructor
    assert hasattr(manager, 'app'), "ConfigurationManager must have 'app' field"
    assert manager.app is app, "ConfigurationManager.app must be the passed app instance"
    
    assert hasattr(manager, 'override_backends'), "ConfigurationManager must have 'override_backends' field"
    assert isinstance(manager.override_backends, dict), "override_backends must be initialized as dict"
    
    assert hasattr(manager, 'loader'), "ConfigurationManager must have 'loader' field"
    assert manager.loader is not None, "loader must be set internally"
    
    assert hasattr(manager, 'conf'), "ConfigurationManager must have 'conf' field"
    assert manager.conf is not None, "conf must be set internally"

def test_configuration_manager_fields_declared_by_init():
    """Test that fields are actually declared by ConfigurationManager.__init__, not set externally."""
    app = Celery("test")
    
    # Create manager - fields should be set by __init__
    manager = ConfigurationManager(app)
    
    # Test that override_backends exists and is a dict (not None, not missing)
    assert 'override_backends' in manager.__dict__, "override_backends must be in manager's __dict__"
    assert isinstance(manager.override_backends, dict), "override_backends must be initialized as dict by __init__"
    
    # Test that loader exists 
    assert 'loader' in manager.__dict__, "loader must be in manager's __dict__"
    
    # Test that conf exists
    assert 'conf' in manager.__dict__, "conf must be in manager's __dict__"
    
    # Test initial state
    assert len(manager.override_backends) == 0, "override_backends should start as empty dict"

# TaskRegistryManager
def test_register_task_adds_task():
    app = Celery()
    manager = TaskRegistryManager(app)

    class MyTask(Task):
        name = "mytask"
        def run(self): return "ok"

    manager.register_task(MyTask())
    assert "mytask" in manager.tasks
    
def test_register_task_raises_on_duplicate():
    app = Celery()
    manager = TaskRegistryManager(app)

    class MyTask(Task):
        name = "mytask"
        def run(self): return "ok"

    manager.register_task(MyTask())

    with pytest.raises(ValueError):
        manager.register_task(MyTask())

def test_get_task_returns_registered_task():
    app = Celery()
    manager = TaskRegistryManager(app)

    class MyTask(Task):
        name = "mytask"

    task = MyTask()
    manager.register_task(task)

    assert manager.get_task("mytask") is task

def test_get_task_raises_keyerror_for_unknown():
    app = Celery()
    manager = TaskRegistryManager(app)

    with pytest.raises(KeyError):
        manager.get_task("not_exist")

def test_has_task_behavior():
    app = Celery()
    manager = TaskRegistryManager(app)

    class MyTask(Task):
        name = "task_a"

    manager.register_task(MyTask())

    assert manager.has_task("task_a") is True
    assert manager.has_task("task_b") is False

def test_finalize_prevents_task_registration():
    app = Celery()
    manager = TaskRegistryManager(app)

    class MyTask(Task):
        name = "task"

    manager.finalize()

    with pytest.raises(RuntimeError):
        manager.register_task(MyTask())

def test_registered_task_is_callable():
    app = Celery()
    manager = TaskRegistryManager(app)

    class MyTask(Task):
        name = "task_callable"

        def run(self, x, y):
            return x + y

    task = MyTask()
    manager.register_task(task)
    result = app.tasks["task_callable"](3, 4)
    assert result == 7

# ResultManager
def test_init_backend_with_valid_uri():
    app = Celery()
    app.conf.result_backend = "rpc://"

    manager = ResultManager(app)
    manager.init_backend()

    backend = manager.get_backend()
    assert backend is not None
    
@pytest.mark.parametrize("invalid_uri", [
    "invalid_backend://",            
    "celery.backends.rpc",            
    "invalid.module.path.Backend",    
])
def test_init_backend_with_invalid_uri(invalid_uri):
    app = Celery()
    app.conf.result_backend = invalid_uri
    manager = ResultManager(app)

    with pytest.raises(ImportError):
        manager.init_backend()

def test_get_backend_before_init():
    app = Celery()
    manager = ResultManager(app)

    with pytest.raises(RuntimeError):
        manager.get_backend()

def test_store_result_with_disabled_backend():
    app = Celery()
    app.conf.result_backend = DisabledBackend(app)

    manager = ResultManager(app)
    manager.init_backend()

    with pytest.raises(RuntimeError):
        manager.store_result("task_id", "value", "SUCCESS")

def test_get_result_with_disabled_backend():
    app = Celery()
    app.conf.result_backend = DisabledBackend(app)

    manager = ResultManager(app)
    manager.init_backend()

    with pytest.raises(RuntimeError):
        manager.get_result("task_id")

# WorkerRunner
class DummyLoader(BaseLoader):
    def __init__(self, app):
        super().__init__(app)
        self.worker_init_called = False
        self.worker_shutdown_called = False
        self.worker_process_init_called = False

    def import_default_modules(self):
        pass

    def on_worker_init(self):
        self.worker_init_called = True

    def on_worker_shutdown(self):
        self.worker_shutdown_called = True

    def on_worker_process_init(self):
        self.worker_process_init_called = True
        
@pytest.fixture
def setup_runner():
    app = Celery("test_app")
    loader = DummyLoader(app)
    runner = WorkerRunner(app, loader)
    return runner, loader

def test_worker_initialization(setup_runner):
    runner, loader = setup_runner
    runner.init_worker()
    assert loader.worker_init_called is True
    
def test_worker_initialization_only_once(setup_runner):
    runner, loader = setup_runner
    runner.init_worker()
    loader.worker_init_called = False
    runner.init_worker()
    assert loader.worker_init_called is False

def test_worker_shutdown(setup_runner):
    runner, loader = setup_runner
    runner.shutdown_worker()
    assert loader.worker_shutdown_called is True

def test_worker_process_init(setup_runner):
    runner, loader = setup_runner
    runner.init_worker_process()
    assert loader.worker_process_init_called is True
    
# integration test
def test_celery_with_all_managers(tmp_path, monkeypatch):
    config_path = tmp_path / "testconfig.py"
    config_path.write_text("broker_url = 'amqp://localhost'")
    monkeypatch.setenv("CELERY_CONFIG_MODULE", "testconfig")
    monkeypatch.syspath_prepend(str(tmp_path))

    app = Celery("test")
    app.config_manager.read_configuration()
    app.config_manager.config_from_object("testconfig")
    assert app.conf["broker_url"] == "amqp://localhost"

    @app.task(name="add")
    def add(x, y):
        return x + y

    assert app.task_registry.has_task("add")
    task = app.task_registry.get_task("add")
    assert task(2, 3) == 5

    backend = app.result_manager.get_backend()
    task_id = "dummy-task-id"
    app.result_manager.store_result(task_id, 10, "SUCCESS")
    result = app.result_manager.get_result(task_id)
    assert result.result == 10
    assert result.status == "SUCCESS"

    app.worker_runner.init_worker()
    assert app.worker_runner.loader.worker_initialized is True

    app.worker_runner.shutdown_worker()
    
    called = {}
    def handler(**kwargs):
        called["value"] = kwargs["data"]

    sig = app.signal_manager.register_signal("custom_event")
    app.signal_manager.connect(sig, handler)
    app.signal_manager.emit(sig, data="hello")
    assert called["value"] == "hello"

def test_override_backend_integration_from_config_manager():
    """Test that ResultManager uses override_backends from ConfigurationManager properly."""
    app = Celery()
    
    # Verify ConfigurationManager initializes override_backends as empty dict
    assert hasattr(app.config_manager, 'override_backends'), "ConfigurationManager must declare override_backends"
    assert isinstance(app.config_manager.override_backends, dict), "override_backends must be dict"
    assert len(app.config_manager.override_backends) == 0, "override_backends should start empty"
    
    # Set up override through ConfigurationManager (not direct assignment)
    app.config_manager.override_backends["original://"] = "memory://"
    app.conf.result_backend = "original://"

    # ResultManager should respect the override_backends from ConfigurationManager
    result_manager = ResultManager(app)
    result_manager.init_backend()

    backend = result_manager.get_backend()
    assert isinstance(backend, CacheBackend), "Backend should be overridden to memory:// -> CacheBackend"

def test_override_backend_uri_applied():
    app = Celery()
    app.config_manager.override_backends = {
        "original://": "memory://",
        "amqp://localhost": "rpc://"
    }
    app.conf.result_backend = "original://"

    manager = ResultManager(app)
    manager.init_backend()

    backend = manager.get_backend()
    assert isinstance(backend, CacheBackend)  # memory:// â†’ CacheBackend

def test_override_backend_uri_not_applied():
    app = Celery()
    app.config_manager.override_backends = {
        "redis://localhost": "memory://",
        "amqp://localhost": "db+sqlite://results.sqlite"
    }
    app.conf.result_backend = "rpc://"

    manager = ResultManager(app)
    manager.init_backend()

    backend = manager.get_backend()
    assert isinstance(backend, RPCBackend)  # rpc:// not overridden