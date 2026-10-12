import { LimeSyncHome } from '~/features/home/LimeSyncHome'
import { OrezHome } from '~/features/home/OrezHome'
import { isLimeSyncSite } from '~/lib/site-config'

export default function HomePage() {
  return isLimeSyncSite ? <LimeSyncHome /> : <OrezHome />
}
