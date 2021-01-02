from build import make, fset

tpl = fset('hisser/*.tpl', '%.tpl', '%')
cython_c = fset('hisser/*.pyx', '%.pyx', '%.c')
cython_cxx = fset('hisser/*.pyxx', '%.pyxx', '%.cpp')
c_ext = fset(cython_c.dest + cython_cxx.dest, '%', '%.so')

make(tpl, deps='hisser/aggop.macro', shell=True,
     cmd='python render-tpl.py {req} {target}')

make(cython_c,
     cmd='cython -3 -a {req}')

make(cython_cxx,
     cmd='cython -3 --cplus -a {req}')

make(c_ext, shell=True,
     cmd='HISSER_BUILD_EXT={req} python setup.py build_ext --inplace')

make('all', c_ext.dest, phony=True)
