import { LiteSyncHome } from '~/features/home/LiteSyncHome'
import { OrezHome } from '~/features/home/OrezHome'
import { isLiteSyncSite } from '~/lib/site-config'

export default function HomePage() {
  return isLiteSyncSite ? <LiteSyncHome /> : <OrezHome />
}
