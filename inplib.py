def get(job_name, mae):
    lib = {
        'primeMMGBSA':primeMMGBSA(mae)
    }
    return lib[job_name]

def primeMMGBSA(mae):
    return [f'STRUCT_FILE	{mae}\n',
            'JOB_TYPE	REAL_MIN\n',
            'LCONS	SMARTS.C']