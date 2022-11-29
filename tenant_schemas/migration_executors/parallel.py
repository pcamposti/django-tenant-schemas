import os
import subprocess
import functools
import multiprocessing

from django.conf import settings

from tenant_schemas.migration_executors.base import MigrationExecutor, run_migrations


class ParallelExecutor(MigrationExecutor):
    codename = 'parallel'

    def run_tenant_migrations(self, tenants):
        db = self.options.get('db', None) or self.options.get('database', None)
        os.system(f"echo {db}")
        os.system(f"echo {self.args}")
        if tenants:
            count = 0
            command = ''
            for tenant in tenants:
                os.system(f"echo {tenant}")
                #command += f'python manage.py migrate_schemas tenant --database={db} --schema={tenant} & \n'
                #command += f'python manage.py migrate_schemas commons_pg --database={db} --schema={tenant} & \n'
                count += 1
                migrate_tenant = subprocess.Popen(['python', 'manage.py', 'migrate_schemas', 'tenant', f'--database={db}', f'--schema={tenant}'])
                migrate_commons = subprocess.Popen(['python', 'manage.py', 'migrate_schemas', 'commons_pg', f'--database={db}', f'--schema={tenant}'])
                os.system(f"echo python manage.py migrate_schemas tenant --database={db} --schema={tenant} ")
                os.system(f"echo python manage.py migrate_schemas commons_pg --database={db} --schema={tenant}")
                if count == 5:
                    migrate_tenant.wait()
                    migrate_commons.wait()
                    count = 0
            command += 'wait \n'
            migrate_commons.wait()
            migrate_tenant.wait()
            os.system(f"echo finish Migrations")
            #os.system(command)
