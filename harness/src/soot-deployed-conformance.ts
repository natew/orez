// Deployed conformance for Soot's OWN rust sync-cf-host composition (not the
// generic harness fixture): a faithful port of soot's integration
// test/workerd.test.ts, driven against the DEPLOYED worker on the LSLCF test
// account instead of a locally-spawned workerd. Closes the M4a exit-gate line
// "deployed conformance stays green using Soot's DEPLOYED composition".
//
// Soot's composition speaks a different dialect than the fixture lanes, which
// is why the generic eviction/reconnect/storm lanes cannot target it as-is:
//   - auth is the `x-soot-test-user` header (SOOT_SYNC_TEST_AUTH=1), not the
//     fixture's `Bearer token-<user>`.
//   - the schema + mutators are soot's (userState/snapshot/message/thread/…),
//     not the fixture's project/task.
//   - namespaces are `soot` (control) and `proj-<id>` / `p-<id>` (project).
// So conformance here reproduces soot's own control/project/wake assertions.
//
// Fresh state: project checks use a per-run `p-drill-<stamp>` namespace; the
// control plane namespace is the singleton `soot`, so control checks use
// per-run drill-prefixed user + row ids and assert only their own rows (soot's
// visibility is own-row / public), never global counts. No production route.
const WORKER =
  process.env.ZHARNESS_SOOT_CF_WORKER ?? 'https://soot-rust-sync-prep.lslcf.workers.dev'
const ADMIN_KEY = process.env.ZHARNESS_SOOT_ADMIN_KEY ?? 'local-soot-sync-admin'

const stamp = `${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`
const projectNs = `p-drill-${stamp}`
const projectID = `drill-${stamp}`
const userA = `drill-a-${stamp}`
const userB = `drill-b-${stamp}`
const reader = `drill-r-${stamp}`

const checks: Array<{ name: string; ok: boolean; detail?: string }> = []
function check(name: string, ok: boolean, detail?: string) {
  checks.push({ name, ok, detail })
  if (!ok) console.error(`  FAIL ${name}${detail ? `: ${detail}` : ''}`)
}
function sqlString(value: string) {
  return `'${value.replaceAll("'", "''")}'`
}

async function admin(namespace: string, query: string) {
  const response = await fetch(`${WORKER}/${namespace}/admin/sql`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-admin-key': ADMIN_KEY },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(15_000),
  })
  if (!response.ok) throw new Error(`admin ${namespace} failed ${response.status}`)
  return (await response.json()) as { rows: Array<Record<string, any>> }
}

async function post(
  namespace: string,
  route: 'pull' | 'push',
  body: unknown,
  userID: string,
  extraHeaders: Record<string, string> = {}
) {
  const response = await fetch(`${WORKER}/${namespace}/${route}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-soot-test-user': userID,
      ...extraHeaders,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  })
  return {
    status: response.status,
    body: (await response.json().catch(() => ({}))) as Record<string, any>,
  }
}

function pullBody(clientID: string, cookie: number | null = null) {
  return { clientID, clientGroupID: `group-${clientID}`, cookie }
}
function mutation(
  clientID: string,
  id: number,
  name: string,
  args: Record<string, unknown>
) {
  return {
    clientGroupID: `group-${clientID}`,
    pushVersion: 1,
    mutations: [{ type: 'custom', clientID, id, name, args: [args] }],
  }
}
function puts(body: Record<string, any>, tableName: string) {
  return (body.rowsPatch ?? []).filter(
    (e: any) => e.op === 'put' && e.tableName === tableName
  )
}

async function controlPlane() {
  // seed own + other user's userState, plus a public deploySlug
  await admin(
    'soot',
    `INSERT INTO "userState" ("userId", "currentProjectId") VALUES (${sqlString(userA)}, NULL), (${sqlString(userB)}, NULL)`
  )
  await admin(
    'soot',
    `INSERT INTO "deploySlug" (slug, "projectId", "userId") VALUES (${sqlString(`slug-${stamp}`)}, ${sqlString(projectID)}, ${sqlString(userB)})`
  )

  const initial = await post('soot', 'pull', pullBody(`c-${userA}`), userA)
  check('control pull status 200', initial.status === 200, String(initial.status))
  const myState = puts(initial.body, 'userState').filter(
    (e: any) => e.value.userId === userA
  )
  const otherState = puts(initial.body, 'userState').filter(
    (e: any) => e.value.userId === userB
  )
  check('control own userState visible', myState.length === 1)
  check(
    "control other user's userState hidden (own-row visibility)",
    otherState.length === 0
  )
  check(
    'control public deploySlug visible',
    puts(initial.body, 'deploySlug').some((e: any) => e.value.slug === `slug-${stamp}`)
  )
  check(
    'control plane excludes project-plane snapshot rows',
    puts(initial.body, 'snapshot').length === 0
  )

  const pushed = await post(
    'soot',
    'push',
    mutation(`c-${userA}`, 1, 'userState.update', {
      userId: userA,
      currentProjectId: projectID,
    }),
    userA
  )
  check(
    'control userState.update mutator acked',
    pushed.body?.pushResponse?.mutations?.[0]?.result &&
      Object.keys(pushed.body.pushResponse.mutations[0].result).length === 0
  )
  const stored = await admin(
    'soot',
    `SELECT "currentProjectId" FROM "userState" WHERE "userId" = ${sqlString(userA)}`
  )
  check(
    'control userState.update persisted',
    stored.rows[0]?.currentProjectId === projectID
  )
  // the generated permission requires the target row to belong to the caller:
  // updating another user's row must be denied
  const forged = await post(
    'soot',
    'push',
    mutation(`c-${userA}`, 2, 'userState.update', {
      userId: userB,
      currentProjectId: projectID,
    }),
    userA
  )
  const forgedResult = forged.body?.pushResponse?.mutations?.[0]?.result
  const otherAfter = await admin(
    'soot',
    `SELECT "currentProjectId" FROM "userState" WHERE "userId" = ${sqlString(userB)}`
  )
  check(
    'control userState.update cross-user write denied',
    otherAfter.rows[0]?.currentProjectId === null,
    JSON.stringify(forgedResult)
  )
}

async function projectPlane() {
  const ns = projectNs
  await admin(
    ns,
    `INSERT INTO project (id, "accountId", "userId") VALUES (${sqlString(projectID)}, ${sqlString(`acct-${stamp}`)}, ${sqlString(userA)})`
  )
  await admin(
    ns,
    `INSERT INTO "accountResourceGrant" (id, "accountId", "resourceType", "resourceId", "userId", role) VALUES (${sqlString(`grant-${stamp}`)}, ${sqlString(`acct-${stamp}`)}, 'project', ${sqlString(projectID)}, ${sqlString(userA)}, 'editor')`
  )
  await admin(
    ns,
    `INSERT INTO "attachCommand" (id, "projectId", "userId") VALUES ('own-${stamp}', ${sqlString(projectID)}, ${sqlString(userA)}), ('other-${stamp}', ${sqlString(projectID)}, ${sqlString(userB)})`
  )
  await admin(
    ns,
    `INSERT INTO "projectAddon" (id, "projectId", "accountId") VALUES ('addon-${stamp}', ${sqlString(projectID)}, ${sqlString(`acct-${stamp}`)})`
  )

  const initial = await post(ns, 'pull', pullBody(`p-${userA}`), userA)
  check('project pull status 200', initial.status === 200, String(initial.status))
  check(
    'project attachCommand own-row only',
    JSON.stringify(puts(initial.body, 'attachCommand').map((e: any) => e.value.id)) ===
      JSON.stringify([`own-${stamp}`])
  )
  check(
    'project projectAddon hidden (full-org gate, empty in project DO)',
    puts(initial.body, 'projectAddon').length === 0
  )
  check(
    'project control-mirrored project row not synced',
    puts(initial.body, 'project').length === 0
  )

  const snap = await post(
    ns,
    'push',
    mutation(`p-${userA}`, 1, 'snapshot.insert', {
      id: `snap-${stamp}`,
      projectId: projectID,
      userId: userA,
      name: 'before',
      fileCount: 2,
      createdAt: 100,
    }),
    userA
  )
  check(
    'project snapshot.insert acked',
    Object.keys(snap.body?.pushResponse?.mutations?.[0]?.result ?? { x: 1 }).length === 0
  )
  const msg = await post(
    ns,
    'push',
    mutation(`p-${userA}`, 2, 'message.sendMainBean', {
      id: `msg-${stamp}`,
      projectId: projectID,
      userId: userA,
      text: 'hello',
      createdAt: 200,
    }),
    userA
  )
  check(
    'project message.sendMainBean acked',
    Object.keys(msg.body?.pushResponse?.mutations?.[0]?.result ?? { x: 1 }).length === 0
  )
  const cross = await admin(
    ns,
    `SELECT (SELECT COUNT(*) FROM thread) AS threads, (SELECT COUNT(*) FROM message) AS messages, (SELECT COUNT(*) FROM snapshot) AS snapshots`
  )
  check(
    'project cross-table mutator wrote thread+message+snapshot',
    cross.rows[0]?.threads >= 1 &&
      cross.rows[0]?.messages >= 1 &&
      cross.rows[0]?.snapshots >= 1,
    JSON.stringify(cross.rows[0])
  )

  const badUpdate = await post(
    ns,
    'push',
    mutation(`p-${userA}`, 3, 'snapshot.update', { id: `snap-${stamp}` }),
    userA
  )
  check(
    'project snapshot.update of existing returns app error',
    badUpdate.body?.pushResponse?.mutations?.[0]?.result?.error === 'app',
    JSON.stringify(badUpdate.body?.pushResponse?.mutations?.[0]?.result)
  )

  const readerPull = await post(ns, 'pull', pullBody(`p-${reader}`), reader)
  check(
    'project reader sees committed snapshot',
    puts(readerPull.body, 'snapshot').some((e: any) => e.value.id === `snap-${stamp}`)
  )

  const denied = await post(ns, 'pull', pullBody(`p-${userB}`), userB, {
    'x-soot-test-project-access': 'deny',
  })
  check('project access-deny returns 401', denied.status === 401, String(denied.status))
}

async function wakePlane() {
  const ns = projectNs
  const url = `${WORKER.replace('https:', 'wss:').replace('http:', 'ws:')}/${ns}/wake?clientID=wake-reader-${stamp}`
  const socket = new WebSocket(url)
  const received = new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('wake timed out')), 8_000)
    socket.addEventListener('message', (e) => {
      clearTimeout(timer)
      resolve(String((e as MessageEvent).data))
    })
    socket.addEventListener('error', () => {
      clearTimeout(timer)
      reject(new Error('wake socket error'))
    })
  })
  await new Promise<void>((resolve, reject) => {
    socket.addEventListener('open', () => resolve(), { once: true })
    socket.addEventListener('error', () => reject(new Error('wake open failed')), {
      once: true,
    })
  })
  const pushed = await post(
    ns,
    'push',
    mutation(`wake-writer-${stamp}`, 1, 'snapshot.insert', {
      id: `wake-${stamp}`,
      projectId: projectID,
      userId: userA,
      name: 'wake',
      fileCount: 1,
      createdAt: 300,
    }),
    userA
  )
  check('wake: push acked', pushed.status === 200)
  try {
    const frame = await received
    check('wake: reader woken immediately, pusher excluded', frame === 'wake', frame)
  } catch (error) {
    check('wake: reader woken immediately, pusher excluded', false, String(error))
  }
  socket.close()
}

try {
  console.log(`[soot-deployed-conformance] worker=${WORKER} projectNs=${projectNs}`)
  await controlPlane()
  await projectPlane()
  await wakePlane()
} catch (error) {
  check('runner completed without throwing', false, String(error))
}

const failed = checks.filter((c) => !c.ok)
console.log(
  JSON.stringify({
    lane: 'soot-deployed-conformance',
    result: failed.length === 0 ? 'PASS' : 'FAIL',
    worker: WORKER,
    projectNamespace: projectNs,
    checks: checks.length,
    failed: failed.length,
    ...(failed.length ? { failures: failed.map((c) => c.name) } : {}),
  })
)
if (failed.length) process.exit(1)
