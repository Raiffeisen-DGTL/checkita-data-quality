package ru.raiffeisen.checkita.config.appconf

import eu.timepit.refined.types.string.NonEmptyString
import ru.raiffeisen.checkita.config.RefinedTypes.URL

/**
 * Application-level configuration describing connection to Mattermost API
 * @param host Mattermost API host
 * @param token Mattermost API token (using Bot accounts for notifications is preferable)
 */
final case class MattermostConfig(
                                   host: URL,
                                   token: NonEmptyString
                                 )
