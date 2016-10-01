package org.dcache.gplazma.plugins

import scala.collection.JavaConversions._

import java.util
import java.util.Properties
import java.security.Principal

import org.dcache.auth.Subjects
import org.dcache.gplazma.AuthenticationException
import scala.io.Source

object BanFilePlugin {
  val BAN_FILE = "gplazma.banfile.path"
}

class BanFilePlugin(properties : Properties) extends GPlazmaAccountPlugin with FileCache[Set[Principal]] {

  /**
   * Get the filename of the ban file from the properties.
   */
  val banFile = {
    if (properties == null) {
      throw new IllegalArgumentException("properties is null")
    }
    val filename = properties getProperty BanFilePlugin.BAN_FILE
    if (filename == null) {
      throw new IllegalArgumentException(BanFilePlugin.BAN_FILE + " not set")
    }

    filename
  }

  private[plugins] def fromSource : Source = try {
    Source fromFile banFile
  } catch {
    case e:Exception => throw new IllegalStateException("cannot read file " + banFile +": "+e.getMessage, e)
  }

  /**
   * Create a list of principals from the source file.
   * principalsFromSource filters out empty lines and comments, i.e., lines starting with #
   * It expects the file to be of the format:
   *   alias <alias>=<full qualified classname>
   *   ban <full qualified classname or alias>:<principal string>
   * e.g.,
   *   alias username=org.dcache.auth.UserNamePrincipal
   *   ban username:Someuser
   * or
   *   ban org.dcache.auth.UserNamePrincipal:Someuser
   *
   * @return a set of banned principals
   */
  private def principalsFromFile(filename : String) = {

    def filteredLines(lines : List[String], filtered : List[String], aliases : Map[String, String]) : List[String] = {
      lines match {
        case Nil => filtered
        case line :: rest if line startsWith "#" => filteredLines(rest, filtered, aliases)
        case line :: rest if line.trim == "" => filteredLines(rest, filtered, aliases)
        case line :: rest if line startsWith "alias" => {
          """^alias\s+([^:]+)=(.*)$""".r("alias", "class") findFirstMatchIn line match {
            case None => throw new IllegalArgumentException("Bad alias line format: '"+line+"', expected 'alias <alias>=<class>'")
            case Some(m) => filteredLines(rest, filtered, aliases + (m.group("alias").trim -> m.group("class").trim))
          }
        }
        case line :: rest if line startsWith "ban" => {
          """^ban\s+([^:]+):(.*)$""".r("class", "params") findFirstMatchIn line match {
            case None => throw new IllegalArgumentException("Bad ban line format: '"+line+"', expected 'ban <classOrAlias>:<value>'")
            case Some(m) => filteredLines(rest, {
              aliases.get(m.group("class").trim) match {
                case None => m.group("class").trim
                case Some(a) => a
              }
            }+":"+m.group("params") :: filtered, aliases)
          }
        }
        case line :: _ => throw new IllegalArgumentException("Line has bad format: '"+line+"', expected '[alias|ban] <key>:<value>'")
      }
    }

    Subjects.principalsFromArgs(filteredLines(fromSource.getLines().toList, Nil, Map())).toSet
  }

  /**
   * Get banned principals from file
   * @return a set of banned principals
   */
  private def bannedPrincipals = getOrFetch(banFile)(principalsFromFile)

  /**
   * Check if any of the principals in authorizedPrincipals is blacklisted in the
   * file specified by the dCache property gplazma.banfile.uri.
   *
   * @param authorizedPrincipals principals associated with a user
   * @throws AuthenticationException indicating a banned user
   */
  def account(authorizedPrincipals: util.Set[Principal]) {
    if ((authorizedPrincipals intersect bannedPrincipals).nonEmpty) {
      throw new AuthenticationException("user banned")
    }
  }
}

