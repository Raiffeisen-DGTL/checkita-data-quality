package org.checkita.dqf.config.appconf

import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.dqf.config.RefinedTypes.URL

/**
 * Application-level configuration describing connection to Mattermost API
 * @param host Mattermost API host
 * @param token Mattermost API token (using Bot accounts for notifications is preferable)
 */
final case class MattermostConfig(
                                   host: URL,
                                   token: NonEmptyString
                                 )
