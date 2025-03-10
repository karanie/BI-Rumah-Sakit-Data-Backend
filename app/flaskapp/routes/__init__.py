from .pendapatan import routes_pendapatan
from .kunjungan import routes_kunjungan
from .pasien import routes_pasien
from .demografi import routes_demografi
from .autogen import routes_autogen
from .utility import routes_utils
from .test import routes_test

def init_app(app):
    app.register_blueprint(routes_pasien)
    app.register_blueprint(routes_kunjungan)
    app.register_blueprint(routes_pendapatan)
    app.register_blueprint(routes_demografi)
    app.register_blueprint(routes_autogen)
    app.register_blueprint(routes_utils)
    app.register_blueprint(routes_test)
