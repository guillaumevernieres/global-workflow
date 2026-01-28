from applications.applications import AppConfig
from rocoto.tasks import Tasks
from wxflow import timedelta_to_HMS, to_timedelta
import rocoto.rocoto as rocoto
import numpy as np


class GFSTasks(Tasks):

    def __init__(self, app_config: AppConfig, run: str) -> None:
        super().__init__(app_config, run)

    @staticmethod
    def _is_this_a_gdas_task(run, task_name):
        if run != 'enkfgdas':
            raise TypeError(f'{task_name} must be part of the "enkfgdas" cycle and not {run}')

    # Specific Tasks begin here
    def fetch(self):

        cycledef = 'gdas_half' if self.run in ['gdas', 'enkfgdas'] else self.run

        resources = self.get_resource('fetch')
        task_name = f'{self.run}_fetch'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/fetch.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def stage_ic(self):

        dependencies = None
        if self.options['do_fetch_hpss'] or self.options['do_fetch_local']:
            deps = []
            dep_dict = {
                'type': 'task', 'name': f'{self.run}_fetch',
            }
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep=deps)

        cycledef = 'gdas_half' if self.run in ['gdas', 'enkfgdas'] else self.run

        resources = self.get_resource('stage_ic')
        task_name = f'{self.run}_stage_ic'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/stage_ic.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;',
                     'dependency': dependencies
                     }

        task = rocoto.create_task(task_dict)

        return task

    def prep_sfc(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'gdas_atmos_prod', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        atm_hist_path = self._template_to_rocoto_cycstring(self._base["COM_ATMOS_HISTORY_TMPL"], {'RUN': 'gdas'})
        data = f'{atm_hist_path}/gdas.t@Hz.atm.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dump_path}/{self.run}.t@Hz.updated.status.tm00.bufr_d'
        dep_dict = {'type': 'data', 'data': data}
        if self.options['do_jediatmvar']:
            data = f'{ioda_path}/atmos/{self.run}.t@Hz.obsforge_atmos_bufr_status.log'
            dep_dict = {'type': 'data', 'data': data}
            deps.append(rocoto.add_dependency(dep_dict))
        # TODO enable this for marine observations when ready
        # if self.options['do_jediocnvar']:
        #     data = f'{ioda_path}/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        #     dep_dict = {'type': 'data', 'data': data}
        #     deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_aero_anl']:
            data = f'{ioda_path}/chem/{self.run}.t@Hz.obsforge_aod_status.log'
            dep_dict = {'type': 'data', 'data': data}
            deps.append(rocoto.add_dependency(dep_dict))
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'metatask', 'name': 'gdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_prep_sfc']:
            dep_dict = {'type': 'task', 'name': f'{self.run}_prep_sfc'}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and self._base['INTERVAL_GFS'] != 6:
            cycledef = 'gdas'

        resources = self.get_resource('prep')
        task_name = f'{self.run}_prep'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prep.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def waveinit(self):

        resources = self.get_resource('waveinit')
        dependencies = None
        cycledef = 'gdas_half,gdas' if self.run in ['gdas'] else self.run
        if self.app_config.mode in ['cycled']:
            deps = []
            dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
            deps.append(rocoto.add_dependency(dep_dict))
            if self.run in ['gdas']:
                dep_dict = {'type': 'cycleexist', 'condition': 'not', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
                deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='or', dep=deps)

        task_name = f'{self.run}_waveinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/waveinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def waveprep(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_waveinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)
        cycledef = 'gdas_half,gdas' if self.run in ['gdas'] else self.run
        resources = self.get_resource('waveprep')
        task_name = f'{self.run}_waveprep'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/waveprep.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aerosol_init(self):

        input_path = self._template_to_rocoto_cycstring(self._base['COM_ATMOS_INPUT_TMPL'])
        restart_path = self._template_to_rocoto_cycstring(self._base['COM_ATMOS_RESTART_TMPL'])

        deps = []
        # Files from current cycle
        ntiles = self._base['ntiles']
        files = ['gfs_ctrl.nc'] + [f'gfs_data.tile{tile}.nc' for tile in range(1, ntiles + 1)]
        for file in files:
            data = f'{input_path}/{file}'
            dep_dict = {'type': 'data', 'data': data}
            deps.append(rocoto.add_dependency(dep_dict))

        # Calculate offset based on RUN = gfs | gdas
        interval = None
        if self.run in ['gfs']:
            interval = self._base['interval_gfs']
        elif self.run in ['gdas']:
            interval = self._base['interval']
        offset = timedelta_to_HMS(-interval)

        # Files from previous cycle
        files = ['@Y@m@d.@H0000.fv_core.res.nc'] + \
                [f'@Y@m@d.@H0000.fv_core.res.tile{tile}.nc' for tile in range(1, self.n_tiles + 1)] + \
                [f'@Y@m@d.@H0000.fv_tracer.res.tile{tile}.nc' for tile in range(1, self.n_tiles + 1)]

        for file in files:
            data = [f'{restart_path}/', file]
            dep_dict = {'type': 'data', 'data': data, 'offset': [offset, None]}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        cycledef = 'gfs_seq'
        resources = self.get_resource('aerosol_init')
        task_name = f'{self.run}_aerosol_init'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aerosol_init.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def anal(self):
        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('anal')
        task_name = f'{self.run}_anal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/anal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def sfcanl(self):

        deps = []
        if self.options['do_jediatmvar']:
            dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfinal'}
        else:
            dep_dict = {'type': 'task', 'name': f'{self.run}_anal'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_jedisnowda']:
            dep_dict = {'type': 'task', 'name': f'{self.run}_snowanl'}
            deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_gsisoilda'] and self.run in ['gdas']:
            dep_dict = {'type': 'task', 'name': 'enkfgdas_eupd'}
            deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_jedisnowda'] or (self.options['do_gsisoilda'] and self.run in ['gdas']):
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('sfcanl')
        task_name = f'{self.run}_sfcanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/sfcanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def analcalc(self):

        deps = []
        if self.options['do_jediatmvar'] and not self.options['do_jediatmens']:
            dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfinal'}
        else:
            dep_dict = {'type': 'task', 'name': f'{self.run}_anal'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_sfcanl'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar'] and self.run in ['gdas']:
            dep_dict = {'type': 'task', 'name': 'enkfgdas_echgres', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('analcalc')
        task_name = f'{self.run}_analcalc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/analcalc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def analdiag(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_anal'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('analdiag')
        task_name = f'{self.run}_analdiag'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/analdiag.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlvar(self):

        deps = []
        dep_dict = {
            'type': 'task', 'name': 'gdas_aeroanlgenb',
            'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}",
        }
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {
            'type': 'task', 'name': f'{self.run}_aeroanlinit',
        }
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlvar')
        task_name = f'{self.run}_aeroanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_aeroanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlfinal')
        task_name = f'{self.run}_aeroanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlgenb(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': f'{self.run}_fcst'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('aeroanlgenb')
        task_name = f'{self.run}_aeroanlgenb'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': 'gdas_half,gdas',
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlgenb.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def aeroanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': 'gdas_aeroanlgenb'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('aeroanlinit')
        task_name = f'{self.run}_aeroanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/aeroanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/prepoceanobs.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlletkf(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prepoceanobs"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_marinebmat"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marineanlletkf')
        task_name = f'{self.run}_marineanlletkf'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marineanlletkf.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmatinit(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar_ocn']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_fcst', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('marinebmatinit')
        task_name = f'{self.run}_marinebmatinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmatinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marinebmat(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_marinebmatinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('marinebmat')
        task_name = f'{self.run}_marinebmat'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/marinebmat.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlinit(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))
            dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)
        else:
            dependencies = rocoto.create_dependency(dep=deps)

        interval_gfs = self._base["INTERVAL_GFS"]
        gfs_enkf = True if self.options['do_hybvar'] and 'gfs' in self.app_config.ens_runs else False

        cycledef = self.run
        if self.run in ['gfs'] and gfs_enkf and interval_gfs != 6:
            cycledef = 'gdas'

        resources = self.get_resource('atmanlinit')
        task_name = f'{self.run}_atmanlinit'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': cycledef,
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlinit.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def coupledanlinit(self):
        """Coupled atmosphere-marine DA initialization task"""

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f'{self.run}_prepoceanobs'}
        deps.append(rocoto.add_dependency(dep_dict))

        if self.options['do_hybvar']:
            dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn',
                       'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
            deps.append(rocoto.add_dependency(dep_dict))

        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('coupledanlinit')
        task_name = f'{self.run}_coupledanlinit'
        task_dict = {
            'task_name': task_name,
            'resources': resources,
            'dependency': dependencies,
            'envars': self.envars,
            'cycledef': self.run,
            'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/coupledanlinit.sh',
            'job_name': f'{self.pslot}_{task_name}_@H',
            'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
            'maxtries': '&MAXTRIES;'
        }

        task = rocoto.create_task(task_dict)

        return task

    def marineanlvar(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlinit'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlvar')
        task_name = f'{self.run}_atmanlvar'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlvar.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfv3inc(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlvar'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('atmanlfv3inc')
        task_name = f'{self.run}_atmanlfv3inc'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfv3inc.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def atmanlfinal(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_atmanlfv3inc'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('atmanlfinal')
        task_name = f'{self.run}_atmanlfinal'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/atmanlfinal.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)

        return task

    def snowanl(self):

        deps = []
        dep_dict = {'type': 'task', 'name': f'{self.run}_prep'}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep=deps)

        resources = self.get_resource('snowanl')
        task_name = f'{self.run}_snowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/snowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def esnowanl(self):

        deps = []
        dep_dict = {'type': 'metatask', 'name': 'enkfgdas_epmn', 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        dep_dict = {'type': 'task', 'name': f"{self.run.replace('enkf', '')}_prep"}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('esnowanl')
        task_name = f'{self.run}_esnowanl'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     'dependency': dependencies,
                     'envars': self.envars,
                     'cycledef': self.run.replace('enkf', ''),
                     'command': f'{self.HOMEgfs}/dev/job_cards/rocoto/esnowanl.sh',
                     'job_name': f'{self.pslot}_{task_name}_@H',
                     'log': f'{self.rotdir}/logs/@Y@m@d@H/{task_name}.log',
                     'maxtries': '&MAXTRIES;'
                     }

        task = rocoto.create_task(task_dict)
        return task

    def prepoceanobs(self):

        ocean_hist_path = self._template_to_rocoto_cycstring(self._base["COM_OCEAN_HISTORY_TMPL"], {'RUN': 'gdas'})
        dmpdir = self._configs['prepoceanobs']["DMPDIR"]

        deps = []
        data = f'{ocean_hist_path}/gdas.t@Hz.inst.f009.nc'
        dep_dict = {'type': 'data', 'data': data, 'offset': f"-{timedelta_to_HMS(self._base['interval_gdas'])}"}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/{self.run}.t@Hz.obsforge_marine_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        data = f'{dmpdir}/{self.run}.@Y@m@d/@H/ocean/insitu/{self.run}.t@Hz.obsforge_marine_bufr_status.log'
        dep_dict = {'type': 'data', 'data': data}
        deps.append(rocoto.add_dependency(dep_dict))
        dependencies = rocoto.create_dependency(dep_condition='and', dep=deps)

        resources = self.get_resource('prepoceanobs')
        task_name = f'{self.run}_prepoceanobs'
        task_dict = {'task_name': task_name,
                     'resources': resources,
                     '