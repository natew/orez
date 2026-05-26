const globalProcess = ((globalThis as any).process ??= {})

globalProcess.env ??= {}
globalProcess.pid ??= 1
globalProcess.argv ??= []
globalProcess.kill ??= () => true

globalProcess.env.SINGLE_PROCESS = '1'
globalProcess.env.NODE_ENV ??= 'development'

export {}
