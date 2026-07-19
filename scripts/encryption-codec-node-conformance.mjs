#!/usr/bin/env node
import { runEncryptionConformance } from 'orez/zero-http/encryption/conformance'

const result = await runEncryptionConformance()
console.log(`OREZ_ENCRYPTION_CONFORMANCE_PASS runtime=node ${JSON.stringify(result)}`)
