package org.dcache.gplazma.plugins

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

import javax.naming.{Context, CommunicationException, NamingEnumeration, NamingException}
import javax.naming.directory.BasicAttributes
import javax.naming.directory.SearchControls
import javax.naming.directory.SearchResult
import javax.naming.ldap.InitialLdapContext

import java.security.Principal
import java.util.Properties

import org.dcache.auth.GidPrincipal
import org.dcache.auth.GroupNamePrincipal
import org.dcache.auth.UidPrincipal
import org.dcache.auth.UserNamePrincipal
import org.dcache.auth.attributes.HomeDirectory
import org.dcache.auth.attributes.ReadOnly
import org.dcache.auth.attributes.RootDirectory
import org.dcache.gplazma.AuthenticationException
import org.dcache.gplazma.NoSuchPrincipalException

/**
 * gPlazma plug-in which uses LDAP server to provide requested information.
 *
 * Can be combined with other map/auth plugins:
 * <pre>
 * auth optional  x509
 * auth optional  voms
 * map  optional  gridmap
 * map  optional krb5
 * map requisite ldap
 * identity requisite ldap
 * session requisite ldap
 * </pre>
 *
 * Corresponding configuration in <b>dcache.conf</b>
 * <pre>
 * gplazma.ldap.server = ldap.example.com
 * gplazma.ldap.port = 389
 * gplazma.ldap.organization = o=SITE,c=CONTRY
 * gplazma.ldap.tree.people = People
 * gplazma.ldap.tree.groups = Groups
 * </pre>
 *
 * @since 2.3
 */
object Ldap {
  val _log: Logger = LoggerFactory.getLogger(classOf[Ldap])
  val GID_NUMBER_ATTRIBUTE = "gidNumber"
  val HOME_DIR_ATTRIBUTE = "homeDirectory"
  val UID_NUMBER_ATTRIBUTE = "uidNumber"
  val COMMON_NAME_ATTRIBUTE = "cn"
  val USER_ID_ATTRIBUTE = "uid"
  val MEMBER_UID_ATTRIBUTE = "memberUid"
  val LDAP_SERVER = "gplazma.ldap.server"
  val LDAP_PORT = "gplazma.ldap.port"
  val LDAP_ORG = "gplazma.ldap.organization"
  val LDAP_PEOPLE_TREE = "gplazma.ldap.tree.people"
  val LDAP_GROUP_TREE = "gplazma.ldap.tree.groups"
  val LDAP_USER_FILTER = "gplazma.ldap.userfilter"
}

class Ldap(properties : Properties) extends GPlazmaIdentityPlugin with GPlazmaSessionPlugin with GPlazmaMappingPlugin {

  private val log = LoggerFactory.getLogger(Ldap.getClass)

  private def newContext = {
    val server = properties.getProperty(Ldap.LDAP_SERVER)
    val port = properties.getProperty(Ldap.LDAP_PORT)
    val env: Properties = new Properties
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.PROVIDER_URL, "ldap://" + server + ":" + port)
    new InitialLdapContext(env, null)
  }

  private var ctx = newContext

  private val userFilter = properties.getProperty(Ldap.LDAP_USER_FILTER)

  private val peopleOU = {
    val organization = properties.getProperty(Ldap.LDAP_ORG)
    val peopleTree = properties.getProperty(Ldap.LDAP_PEOPLE_TREE)
    String.format("ou=%s,%s", peopleTree, organization)
  }

  private val groupOU = {
    val organization = properties.getProperty(Ldap.LDAP_ORG)
    val groupTree = properties.getProperty(Ldap.LDAP_GROUP_TREE)
    String.format("ou=%s,%s", groupTree, organization)
  }

  private def recreateContext() = {
    try {
      ctx.close()
    } catch {
      case _:NamingException =>
    }
    ctx = newContext
  }

  private def retryWithNewContextOnException[T](f : () => NamingEnumeration[SearchResult]) = {
    try {
      f()
    } catch {
      case (e: CommunicationException) => {
        recreateContext()
        f()
      }
    }
  }

  def map(principals: java.util.Set[Principal]) {
    principals.find( p => p.isInstanceOf[UserNamePrincipal] ) match {
      case Some(p) => try {
        val peopleMaps = mapSearchPeopleByName(p).foldLeft(List[Principal]())( (pset, peopleResult) => {
          val userAttr = peopleResult.getAttributes
          new UidPrincipal(userAttr.get(Ldap.UID_NUMBER_ATTRIBUTE).get.asInstanceOf[String]) ::
            new GidPrincipal(userAttr.get(Ldap.GID_NUMBER_ATTRIBUTE).get.asInstanceOf[String], true) ::
            pset
        })
        principals.addAll(peopleMaps)

        val groupMaps = mapSearchGroupByName(p).foldLeft(List[Principal]())( (pset, groupResult) => {
          new GidPrincipal(groupResult.getAttributes.get(Ldap.GID_NUMBER_ATTRIBUTE).get.asInstanceOf[String], false) ::
            pset
        })
        principals.addAll(groupMaps)
      } catch {
        case e: NamingException => {
          log.debug("Failed to get mapping: {}", e.toString)
          throw new AuthenticationException("no mapping")
        }
      }
      case None => throw new AuthenticationException("no username")
    }
  }

  private def mapSearchGroupByName(principal: Principal): NamingEnumeration[SearchResult] = {
    retryWithNewContextOnException[NamingEnumeration[SearchResult]]( () => {
      ctx.search(groupOU,
        new BasicAttributes(Ldap.MEMBER_UID_ATTRIBUTE, principal.getName))
    })
  }

  private def mapSearchPeopleByName(principal: Principal): NamingEnumeration[SearchResult] = {
    retryWithNewContextOnException[NamingEnumeration[SearchResult]]( () => {
      ctx.search(peopleOU,
        String.format(userFilter, principal.getName),
        getSimpleSearchControls(Array(Ldap.UID_NUMBER_ATTRIBUTE, Ldap.GID_NUMBER_ATTRIBUTE)))
    })
  }

  def map(principal: Principal): Principal = (
    try {
      val name: String = principal.getName
      principal match {
        case _: UserNamePrincipal =>
          mapSearchPeopleById(name).collectFirst[Principal]( { case searchResult =>
            new UidPrincipal(searchResult.getAttributes.get(Ldap.UID_NUMBER_ATTRIBUTE).get.asInstanceOf[String])
          })
        case _: GroupNamePrincipal =>
          mapSearchGroupById(name).collectFirst[Principal]( { case searchResult =>
            new GidPrincipal(searchResult.getAttributes.get(Ldap.GID_NUMBER_ATTRIBUTE).get.asInstanceOf[String], false)
          })
        case _ => None
      }
    } catch {
      case e: NamingException => {
        log.debug("Failed to get mapping: {}", e.toString)
        None
      }
    })
  match {
    case Some(p) => p
    case None => throw new NoSuchPrincipalException(principal)
  }


  private def mapSearchGroupById(name: String): NamingEnumeration[SearchResult] = {
    retryWithNewContextOnException( () => {
      ctx.search(groupOU,
        String.format("(%s=%s)", Ldap.COMMON_NAME_ATTRIBUTE, name),
        getSimpleSearchControls(Array(Ldap.GID_NUMBER_ATTRIBUTE)))
    })
  }

  private def mapSearchPeopleById(name: String): NamingEnumeration[SearchResult] = {
    retryWithNewContextOnException( () => {
      ctx.search(peopleOU,
        String.format("(%s=%s)", Ldap.USER_ID_ATTRIBUTE, name),
        getSimpleSearchControls(Array(Ldap.UID_NUMBER_ATTRIBUTE)))
    })
  }

  def reverseMap(principal: Principal) : java.util.Set[Principal] = (
    try {
      val id: String = principal.getName
      principal match {
        case _: GidPrincipal =>
          reverseMapSearchGroup(id).map((searchResult) => {
            new GroupNamePrincipal(
              searchResult.getAttributes.get(Ldap.COMMON_NAME_ATTRIBUTE).get.asInstanceOf[String]
            )
          })
        case _: UidPrincipal =>
          reverseMapSearchUser(id).map((searchResult) => {
              new UserNamePrincipal(
                searchResult.getAttributes.get(Ldap.USER_ID_ATTRIBUTE).get.asInstanceOf[String]
              )
            })
        case _ => Nil
      }
    }
    catch {
      case e: NamingException => {
        log.debug("Failed to get reverse mapping: {}", e.toString)
        Nil
      }
    }
  ).toSet[Principal]

  private def reverseMapSearchUser(id: String): NamingEnumeration[SearchResult] = {
    retryWithNewContextOnException( () => {
      ctx.search(peopleOU,
        new BasicAttributes(Ldap.UID_NUMBER_ATTRIBUTE, id))
    })
  }

  private def reverseMapSearchGroup(id: String): NamingEnumeration[SearchResult] = {
    retryWithNewContextOnException( () => {
      ctx.search(groupOU,
        new BasicAttributes(Ldap.GID_NUMBER_ATTRIBUTE, id))
    })
  }

  def session(authorizedPrincipals: java.util.Set[Principal], attrib: java.util.Set[AnyRef]) {
    val principal = authorizedPrincipals.find(p => p.isInstanceOf[UserNamePrincipal])
    principal match {
      case Some(p) =>
        try {
          val sResult = sessionSearchHome(p)
          sResult.foreach( searchResult => {
            attrib.add(new HomeDirectory(searchResult.getAttributes.get(Ldap.HOME_DIR_ATTRIBUTE).get.asInstanceOf[String]))
            attrib.add(new RootDirectory("/"))
            attrib.add(new ReadOnly(false))
          })
        } catch {
          case e: NamingException => throw new AuthenticationException("no mapping")
        }
      case None => throw new AuthenticationException("no username")
    }
  }

  private def sessionSearchHome(principal: Principal): NamingEnumeration[SearchResult] = {
    retryWithNewContextOnException( () => {
      ctx.search(peopleOU,
        String.format(userFilter, principal.getName),
        getSimpleSearchControls(Array(Ldap.HOME_DIR_ATTRIBUTE)))
    })
  }

  private def getSimpleSearchControls(attr: Array[String]) = {
    val constraints = new SearchControls
    constraints.setSearchScope(SearchControls.SUBTREE_SCOPE)
    constraints.setReturningAttributes(attr)
    constraints
  }
}