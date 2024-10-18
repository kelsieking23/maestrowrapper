import os
from re import sub
import shutil
import sys
import time
import json
import subprocess
from matplotlib.pyplot import plot
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
import threading
from maestrowrapper import inplib
import multiprocessing as mp

class MaestroWrapper:
    def __init__(self, schrodinger, path=None, files=None, prep_onload=False):
        self.schrodinger = schrodinger
        self._files = []
        self.prep_onload = prep_onload
        self.computer = os.environ['COMPUTERNAME']
        self.environ = os.environ.copy()
        pathlist = [schrodinger]
        program_dirs = ['unxutils', 'utilities', 'tools']
        for program_dir in program_dirs:
            pathlist.append(os.path.join(schrodinger, program_dir))
        self.environ['PATH'] += os.pathsep + os.pathsep.join(pathlist)
        if path is not None:
            self.path = os.path.abspath(path)
        else:
            self.path = os.getcwd()
        if files is None:
            self.files = os.listdir(self.path)
        else:
            self.files = files
        self.progress_tracker = 0
        self.queue_ready = False
        self.pending_jobs = []
        self.active_jobs = []
        self.queued_jobs = []
        self.completed_jobs = 0
        self.total_jobs = 0
        self.max_jobs = 4
        self.terminate = False
        self.running = 0

        self.lics_per_job = {
            'PSP_PLOP':8
        }
    
    @property
    def num_active(self):
        return len(self.active_jobs)
    
    @property
    def num_queued(self):
        return len(self.queued_jobs)

    @property
    def num_completed(self):
        return len(self.completed_jobs)
    
    @property
    def num_pending(self):
        return len(self.pending_jobs)


    def divide_files(self, n):
        # Calculate the target size of each smaller list
        target_size = len(self.files) // n
        remainder = len(self.files) % n

        divided_files = []
        start = 0

        for i in range(n):
            end = start + target_size + (1 if i < remainder else 0)
            smaller_list = self.files[start:end]
            divided_files.append(smaller_list)
            start = end

        return divided_files

    def queue(self):
        while True:
            if (not self.queue_ready):
                print('queue waiting... currently running {} jobs'.format(self.running))
                time.sleep(2)
            else:
                if not self.num_pending == 0:
                    print('FOUND JOB IN QUEUE')
                    job = self.pending_jobs.pop(0)
                    self.active_jobs.append(job)
            if self.terminate:
                break

    def run_subjob(self, subjob_params,):
        job_id = subjob_params['job_id']
        files = subjob_params['files']
        cmds = subjob_params['cmds']
        tmpdir = subjob_params['tmpdir']
        lic = subjob_params['lic']
        if not os.path.isdir(tmpdir):
            os.mkdir(tmpdir)
        os.chdir(tmpdir)
        total_jobs = len(cmds)
        completed_jobs = 0
        for file in files:
            shutil.copy2(file, os.getcwd())
        for cmd in cmds:
            while not self.lics_avail(lic, debug=True, job_id=tmpdir):
                time.sleep(3)
            process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environ)
            while not self.is_launched(os.getcwd()):
                time.sleep(3)
            while self.is_launched(os.getcwd()):
                time.sleep(3)
            completed_jobs += 1
            if completed_jobs % 4 == 0:
                print('Subjob {}: {}/{} sub-jobs completed'.format(job_id, completed_jobs, total_jobs))
            
        print('Subjob {}: Complete'.format(job_id))
        os.chdir(self.path)

    def run_cmd(self, cmd):
        process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environ)
        while not self.is_launched(os.getcwd()):
            time.sleep(3)
        while self.is_launched(os.getcwd()):
            time.sleep(3)
        stdout, stderr = process.communicate()
        return stdout, stderr
    
    def is_launched(self, path):
        job_id = '.{}'.format(self.computer)
        for file in os.listdir(path):
            if file.startswith(job_id):
                return True
        return False

    def lics_avail(self, lic, debug=False, job_id=None):
        process = subprocess.Popen(['licadmin', 'stat'], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environ)
        stdout, stderr = process.communicate()
        for line in stdout.decode().split('\n'):
            if line.startswith('Users'):
                _lic = line.split(':')[0].split()[-1]
                if lic.upper() == _lic.upper():
                    issued = int(line.split(';')[0].split('of')[-1].strip().split()[0])
                    inuse = int(line.split(';')[-1].split('of')[-1].strip().split()[0])
                    if debug:
                        if job_id is None:
                            speaker = 'NONE'
                        else:
                            speaker = job_id
                        print('JobID {}: {} issued, {} in use'.format(job_id, issued, inuse))
                    if (inuse == issued) or (inuse == issued-1):
                        return False
        return True
    
    def listener(self, path):
        job_id = '.{}'.format(self.computer)
        running = True
        running_jobs = 0
        while running:
            for file in os.listdir(path):
                if file.startswith(job_id):
                    running_jobs += 1
            if running_jobs > 0:
                continue
            else:
                running = False
            running_jobs = 0
        return 0
    
    def ligprep(self, output_type='sd', nt=4, export_to='ligprep', options = [], kwarg_options= {}):
        path = os.path.join(os.path.dirname(self.path), export_to)
        if not os.path.isdir(path):
            os.mkdir(path)
        os.chdir(path)
        jobs = self.divide_files(nt)
        job_params = []
        for job_index, files in enumerate(jobs):
            tmpdir = 'ligprep{}'.format(job_index)
            subjob_params = {
                'job_id':job_index,
                'files':[os.path.join(self.path, file) for file in files],
                'cmds':[],
                'tmpdir':tmpdir,
                'lic':'LIGPREP_MAIN'
            }
            if not os.path.isdir(tmpdir):
                os.mkdir(tmpdir)
            for file in files:
                basename, ext = os.path.splitext(os.path.basename(file))
                inp_option = '-i{}'.format(ext[1:])
                out_option = '-o{}'.format(output_type)
                out_file = '{}{}'.format(basename, '.{}'.format(output_type))
                cmd = f'ligprep -HOST localhost:12 {inp_option} {file} {out_option} {out_file}'
                for option in options:
                    cmd = cmd + ' {}'.format(option)
                for k, v in kwarg_options.items():
                    cmd = cmd + ' {} {}'.format(k, str(v))
                subjob_params['cmds'].append(cmd)
            print(subjob_params['cmds'][0])
            job_params.append(subjob_params)
        print('There are {} subjobs'.format(nt))
        tj = 0
        for sj in job_params:
            print('Subjob {} has {} jobs'.format(sj['job_id'], len(sj['files'])))
            tj += len(sj['files'])
        print('Total {} jobs to be completed.'.format(tj))
        print('Launching ligprep job(s)...')
        if nt > 1:
            pool = mp.Pool(processes=nt)
            results = pool.map(self.run_subjob, job_params)
        else:
            self.run_subjob(job_params[0])
        print('ligprep complete.')
        print('Cleaning...')
        for job_param in job_params:
            tmpdir = job_param['tmpdir']
            for file in os.listdir(tmpdir):
                src = os.path.join(tmpdir, file)
                dest = os.path.join(path, file)
                shutil.copy2(src, dest)
                os.remove(src)
            os.rmdir(tmpdir)

    @staticmethod
    def getPrepOut(file):
        base, _ = os.path.splitext(file)
        return f'prep_{base}.mae'
    
    @staticmethod
    def mae2pdb(mae, pdb):
        environ = os.environ.copy()
        schrodinger = 'D:/Schrodinger2022-3'
        pathlist = [schrodinger]
        program_dirs = ['unxutils', 'utilities', 'tools']
        for program_dir in program_dirs:
            pathlist.append(os.path.join(schrodinger, program_dir))
        environ['PATH'] += os.pathsep + os.pathsep.join(pathlist)
        cmd = str('pdbconvert -noindex -imae ' + mae + ' -opdb ' + pdb)
        process = subprocess.Popen(cmd.split(), shell=False, stdout=None, stderr=None, env=environ)
        return pdb
    
    def separate_mae(self, mae, basename=None, export_to=None):
        n_structs = 0
        with open(mae, 'r') as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith('f_m_ct'):
                n_structs += 1
        if export_to is None:
            export_to = os.path.dirname(mae)
        outs = []
        for struct in range(1, n_structs+1):
            cmd = 'maesubset -n {} {}'.format(struct, mae)
            process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environ)
            stdout, stderr = process.communicate()
            if basename is None:
                base = os.path.splitext(os.path.basename(mae))[0]
            else:
                base = basename
            out = os.path.join(export_to, '{}_{}.mae'.format(base, struct))
            with open(out, 'w') as f:
                f.write(stdout.decode())
            outs.append(out)
        return outs

    def prepWizard(self, write_pdb=True, options=[], **kwargs):
        print('Starting PrepWizard on {} files...'.format(len(self.files)))
        _home = os.getcwd()
        os.chdir(self.path)
        jobs = range(1, len(self.files)+1)  # Total jobs is number of files
        self.total_jobs = len(self.files)
        self.completed_jobs = 0
        jobs = self.divide_files(4)
        prepwizard_path = os.path.join(self.schrodinger, 'utilities', 'prepwizard.exe')
        job_params = []
        temp_dirs = []
        for (job_index, files) in enumerate(jobs):
            subjob_params = {
                'job_id':job_index,
                'files':[os.path.join(self.path, file) for file in files],
                'cmds':[],
                'tmpdir':'prepwizard{}'.format(job_index),
                'lic':'MAESTRO_MAIN'
            }
            temp_dirs.append('prepwizard{}'.format(job_index))
            for file in files:
                cmd = '{} {} {}'.format(prepwizard_path, file, self.getPrepOut(file))
                for option in options:
                    cmd = cmd + ' {}'.format(option)
                subjob_params['cmds'].append(cmd)
            job_params.append(subjob_params)
        pool = mp.Pool(processes=4)
        print('Launching...')
        results = pool.map(self.run_subjob, job_params)
        print('PrepWizard complete.')
        print('Cleaning...')
        # cleanup 
        if not os.path.isdir('prepped_mae'):
            os.mkdir('prepped_mae')
        if not os.path.isdir('prepwizard_logs'):
            os.mkdir('prepwizard_logs')
        for job_param in job_params:
            tmpdir = job_param['tmpdir']
            for file in os.listdir(tmpdir):
                path = os.path.join(tmpdir, file)
                if os.path.isdir(path):
                    print(path, os.path.join('prepwizard_logs', file))
                    shutil.copytree(path, os.path.join('prepwizard_logs', file))
                    for f in os.listdir(path):
                        os.remove(os.path.join(path, f))
                    os.rmdir(path)
                else:
                    if file.endswith('log'):
                        shutil.copy2(path, os.path.join('prepwizard_logs', file))
                    if file.startswith('prep'):
                        shutil.copy2(path, os.path.join('prepped_mae', file))
                    os.remove(path)
            os.rmdir(tmpdir)
        prepped_mae = os.path.join(self.path, 'prepped_mae')
        if write_pdb:
            print('Writing PDBs...')
            prepped_pdb = os.path.join(self.path, 'prepped_pdb')
            if not os.path.isdir(prepped_pdb):
                os.mkdir(prepped_pdb)
            for file in os.listdir(prepped_mae):
                print(file)
                if (file.startswith('prep')) and (file.endswith('mae')):
                    pdb_basename = os.path.splitext(os.path.basename(file))[0] + '.pdb'
                    mae = os.path.join(self.path, 'prepped_mae', file)
                    pdb = os.path.join(prepped_pdb, pdb_basename)
                    self.mae2pdb(mae, pdb)
        self.path = os.path.join(self.path, 'prepped_mae')
        self._files = [file for file in os.listdir(self.path) if (file.startswith('prep')) and (file.endswith('mae'))]
        os.chdir(_home)

    def concat(self, files=None, output='concat.mae'):
        if files is None:
            files = self.files
        _home = os.getcwd()
        if not _home == self.path:
            os.chdir(self.path)
        header = ['{' ,
        's_m_m2io_version',
        ':::',
        '2.0.0 ',
        '} ']
        with open(output, 'w') as f:
            for line in header:
                f.write(f'{line}\n')
            f.write('\n')
            for file in files:
                with open(file, 'r') as mae:
                    contents = mae.readlines()
                start = False
                for line in contents:
                    if line.startswith('f_m_ct {'):
                        start = True
                        f.write(line)
                    elif start is False:
                        continue
                    else:
                        f.write(line)
        os.chdir(_home)

    def complex(self, files=None, protein='prep_protein.mae', export_to='complex'):
        _home = os.getcwd()
        os.chdir(self.path)
        export_path = os.path.join(os.path.dirname(self.path), export_to)
        if not os.path.isdir(export_path):
            os.mkdir(export_path)
        os.chdir(self.path)
        if files is None:
            files = self.files
        for file in files:
            if file == protein:
                continue
            base = os.path.splitext(file)[0] + '_complex' + os.path.splitext(file)[-1]
            out = os.path.join(export_path, base)
            cmd = ['structcat', '-imae', protein, file, '-omae', out]
            # print(' '.join(cmd))
            process = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environ)
            process.communicate()
        self.path = export_path
        self.files = os.listdir(self.path)
        os.chdir(_home)
    
    @staticmethod
    def getINP(job_name, mae):
        return inplib.get(job_name, mae)

    @staticmethod
    def writeINP(inp, out, **kwargs):
        f = open(out, 'w')
        for line in inp:
            f.write(line)
        for k, v in kwargs.items():
            f.write(f'{k.upper()} {v} \n')
        f.close()

    def qikprop(self, export_to='qikprop', nt=4, options = []):
        path = os.path.join(os.path.dirname(self.path), export_to)
        if not os.path.isdir(path):
            os.mkdir(path)
        os.chdir(path)
        jobs = self.divide_files(nt)
        job_params = []
        for job_index, files in enumerate(jobs):
            tmpdir = 'primeMMGBSA{}'.format(job_index)
            subjob_params = {
                'job_id':job_index,
                'files':[os.path.join(self.path, file) for file in files],
                'cmds':[],
                'tmpdir':tmpdir,
                'lic':'QIKPROP_MAIN'
            }
            if not os.path.isdir(tmpdir):
                os.mkdir(tmpdir)
            for file in files:
                cmd = f'qikprop -HOST localhost:12 {file}'
                for option in options:
                    if not option.startswith('-'):
                        raise ValueError('needs to be a -flag')
                    cmd = cmd + f' {option}'
                subjob_params['cmds'].append(cmd)
            job_params.append(subjob_params)
        print('There are {} subjobs'.format(nt))
        tj = 0
        for sj in job_params:
            print('Subjob {} has {} jobs'.format(sj['job_id'], len(sj['files'])))
            tj += len(sj['files'])
        print('Total {} jobs to be completed.'.format(tj))
        print('Launching qikprop job(s)...')
        if nt > 1:
            pool = mp.Pool(processes=nt)
            results = pool.map(self.run_subjob, job_params)
        else:
            self.run_subjob(job_params[0])
        print('qikprop complete.')
        print('Cleaning...')
        for job_param in job_params:
            tmpdir = job_param['tmpdir']
            for file in os.listdir(tmpdir):
                src = os.path.join(tmpdir, file)
                dest = os.path.join(path, file)
                shutil.copy2(src, dest)
                os.remove(src)
            os.rmdir(tmpdir)

    def primeMMGBSA(self, export_to='primeMMGBSA', nt=4, schrod_kwargs={}):
        path = os.path.join(self.path, export_to)

        if not os.path.isdir(path):
            os.mkdir(path)
        os.chdir(path)
        jobs = self.divide_files(nt)
        job_params = []
        for job_index, files in enumerate(jobs):
            tmpdir = 'primeMMGBSA{}'.format(job_index)
            subjob_params = {
                'job_id':job_index,
                'files':[os.path.join(self.path, file) for file in files],
                'cmds':[],
                'tmpdir':tmpdir,
                'lic':'PSP_PLOP'
            }
            if not os.path.isdir(tmpdir):
                os.mkdir(tmpdir)
            for file in files:
                basename = os.path.splitext(os.path.basename(file))[0]
                inp = os.path.join(tmpdir, f'{basename}.inp')
                self.writeINP(self.getINP('primeMMGBSA', file), inp, **schrod_kwargs)
                cmd = f'prime_mmgbsa -HOST localhost:12 -prime_opt OPLS_VERSION=OPLS3e {os.path.basename(inp)}'
                subjob_params['cmds'].append(cmd)
            job_params.append(subjob_params)
        print('There are {} subjobs'.format(nt))
        tj = 0
        for sj in job_params:
            print('Subjob {} has {} jobs'.format(sj['job_id'], len(sj['files'])))
            tj += len(sj['files'])
        print('Total {} jobs to be completed.'.format(tj))
        print('Launching primeMMGBSA job(s)...')
        if nt > 1:
            pool = mp.Pool(processes=nt)
            results = pool.map(self.run_subjob, job_params)
        else:
            self.run_subjob(job_params[0])
        print('primeMMGBSA complete.')
        print('Cleaning...')
        for job_param in job_params:
            tmpdir = job_param['tmpdir']
            for file in os.listdir(tmpdir):
                src = os.path.join(tmpdir, file)
                dest = os.path.join(path, file)
                shutil.copy2(src, dest)
                os.remove(src)
            os.rmdir(tmpdir)
        self.mmgbsa_path = path
        self.path = path
        self.mmgbsa_concat()

    def prep(self, struct):
        pass


    def mmgbsa_concat(self):
        dfs = []
        names = []
        for file in os.listdir(self.mmgbsa_path):
            if (file == 'mmgbsa_all.csv'):
                continue
            else:
                if file.endswith('csv'):
                    name = file.split('complex')[0][:-1]
                    df = pd.read_csv(os.path.join(self.mmgbsa_path, file))
                    if not df.empty:
                        dfs.append(df)
                        names.append(name)
        df = pd.concat(dfs).reset_index(drop=True)
        df['title'] = names
        print(df)
        df.to_csv('mmgbsa_all.csv')
        return df

    def fingerprint(self, complex=True):
        if complex:
            self.complex()
        _home = os.getcwd()
        csvs = self.run_fingerprint(complex=complex)

    def run_fingerprint(self, complex=True):
        base_path = os.path.dirname(self.path)
        fingerprint_path = os.path.join(base_path, 'fingerprint')
        if not os.path.isdir(fingerprint_path):
            os.mkdir(fingerprint_path)
        for file in self.files:
            src = os.path.join(self.path, file)
            dest = os.path.join(fingerprint_path, file)
            shutil.copy2(src, dest)
        os.chdir(fingerprint_path)
        outs = []
        for file in os.listdir(os.getcwd()):
            print('****************')
            print(file)
            base, _ = os.path.splitext(file)
            out = '{}_fingerprint.csv'.format(base)
            cmd = 'run interaction_fingerprints.py -i {} -ocsv {}'.format(file, out)
            process = subprocess.Popen(cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environ)
            stdout, stderr = process.communicate()
            print(stdout.decode())
            print(stderr.decode())
            f = open(out, 'r')
            contents = f.readlines()
            f.close()
            parts = contents[1].split(',')
            parts[0] = base
            line = ','.join(parts)
            contents[1] = line
            f = open(out, 'w')
            for line in contents:
                f.write(line)
            f.close()
            os.remove(file)
            outs.append(out)
            print('****************')
        return outs



# if __name__ == '__main__':
#     mw = MaestroWrapper('D:/Schrodinger2022-3')
#     mw.separate_mae('D:/Work/students/marion/inhibs_mmgbsa/docked_poses_mae/Title_K1_docking.mae')