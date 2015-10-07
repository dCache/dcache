package org.dcache.gplazma.plugins

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.convert.Wrappers._

import javax.naming.{Context, CommunicationException, NamingEnumeration, NamingException}
import javax.naming.directory.{Attributes, BasicAttributes, SearchControls, SearchResult}
import javax.naming.ldap.InitialLdapContext

import java.security.Principal
import java.util.Properties

import org.dcache.auth.GidPrincipal
import org.dcache.auth.GroupNamePrincipal
import org.dcache.auth.UidPrincipal
import org.dcache.auth.UserNamePrincipal
import org.dcache.auth.attributes._
import org.dcache.gplazma.AuthenticationException
import org.dcache.gplazma.NoSuchPrincipalException
import scala.collection.convert.WrapAsJava

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
 * gplazma.ldap.url = ldap://example.org:389/
 * gplazma.ldap.organization = o=SITE,c=COUNTRY
 * gplazma.ldap.tree.people = People
 * gplazma.ldap.tree.groups = Groups
 * gplazma.ldap.home-dir = "/"
 * gplazma.ldap.root-dir = "%homeDirectory%" evaluates to the users home directory
 * gplazma.ldap.group-member = "memberUid" or "uniqueMember"
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
  val UNIQUE_MEMBER_ATTRIBUTE = "uniqueMember"
  val LDAP_URL = "gplazma.ldap.url"
  val LDAP_ORG = "gplazma.ldap.organization"
  val LDAP_PEOPLE_TREE = "gplazma.ldap.tree.people"
  val LDAP_GROUP_TREE = "gplazma.ldap.tree.groups"
  val LDAP_USER_FILTER = "gplazma.ldap.userfilter"
  val LDAP_USER_HOME = "gplazma.ldap.home-dir"
  val LDAP_USER_ROOT = "gplazma.ldap.root-dir"
  val LDAP_GROUP_MEMBER = "gplazma.ldap.group-member"
}

class Ldap(properties : Properties) extends GPlazmaIdentityPlugin with GPlazmaSessionPlugin with GPlazmaMappingPlugin {

  private val log = LoggerFactory.getLogger(Ldap.getClass)

  private def newContext = {
    val url = properties.getProperty(Ldap.LDAP_URL)
    val env: Properties = new Properties
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.PROVIDER_URL, url)
    new InitialLdapContext(env, null)
  }

  private var ctx = newContext

  private val userFilter = properties.getProperty(Ldap.LDAP_USER_FILTER)
  private val groupMember = properties.getProperty(Ldap.LDAP_GROUP_MEMBER)

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

  private val userHome = {
    properties.getProperty(Ldap.LDAP_USER_HOME)
  }

  private val userRoot = {
    properties.getProperty(Ldap.LDAP_USER_ROOT)
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

  private implicit def attrToString(attr: javax.naming.directory.Attribute) : String = attr.get.asInstanceOf[String]

  def map(principals: java.util.Set[Principal]) {
    JSetWrapper(principals).find( p => p.isInstanceOf[UserNamePrincipal] ) match {
      case Some(p) => try {
        val peopleMaps = mapSearchPeopleByName(p).foldLeft(List[Principal]())( (pset, peopleResult) => {
          val userAttr = peopleResult.getAttributes
          new UidPrincipal(userAttr.get(Ldap.UID_NUMBER_ATTRIBUTE)) ::
            new GidPrincipal(userAttr.get(Ldap.GID_NUMBER_ATTRIBUTE), true) ::
            pset
        })
        principals.addAll(SeqWrapper(peopleMaps))

        val groupMaps = mapSearchGroupByName(p).foldLeft(List[Principal]())( (pset, groupResult) => {
          new GidPrincipal(groupResult.getAttributes.get(Ldap.GID_NUMBER_ATTRIBUTE), false) ::
            pset
        })
        principals.addAll(SeqWrapper(groupMaps))
      } catch {
        case e: NamingException => {
          log.info("Failed to get mapping: {}", e.toString)
          throw new AuthenticationException("no mapping")
        }
      }
      case None => throw new AuthenticationException("no username")
    }
  }

  private def mapSearchGroupByName(principal: Principal) = groupMember match  {
      case Ldap.UNIQUE_MEMBER_ATTRIBUTE => mapSearchGroupByUniqueMember(principal)
      case Ldap.MEMBER_UID_ATTRIBUTE => mapSearchGroupByMemberUid(principal)
  }

  private def mapSearchGroupByMemberUid(principal: Principal) = JEnumerationWrapper {
    retryWithNewContextOnException[NamingEnumeration[SearchResult]]( () => {
      ctx.search(groupOU,
        String.format("%s=%s",Ldap.MEMBER_UID_ATTRIBUTE, principal.getName),
        getSimpleSearchControls(Ldap.GID_NUMBER_ATTRIBUTE))
    })
  }

  private def mapSearchGroupByUniqueMember(principal: Principal) = JEnumerationWrapper {
    retryWithNewContextOnException[NamingEnumeration[SearchResult]]( () => {
      ctx.search(groupOU,
         String.format("%s=uid=%s,%s",Ldap.UNIQUE_MEMBER_ATTRIBUTE, principal.getName,peopleOU),
         getSimpleSearchControls(Ldap.GID_NUMBER_ATTRIBUTE))
    })
  }

  private def mapSearchPeopleByName(principal: Principal) = JEnumerationWrapper {
    retryWithNewContextOnException[NamingEnumeration[SearchResult]]( () => {
      ctx.search(peopleOU,
        String.format(userFilter, principal.getName),
        getSimpleSearchControls(Ldap.UID_NUMBER_ATTRIBUTE, Ldap.GID_NUMBER_ATTRIBUTE))
    })
  }

  def map(principal: Principal): Principal = (
    try {
      val name: String = principal.getName
      principal match {
        case _: UserNamePrincipal =>
          mapSearchPeopleById(name).collectFirst[Principal]( { case searchResult =>
            new UidPrincipal(searchResult.getAttributes.get(Ldap.UID_NUMBER_ATTRIBUTE))
          })
        case _: GroupNamePrincipal =>
          mapSearchGroupById(name).collectFirst[Principal]( { case searchResult =>
            new GidPrincipal(searchResult.getAttributes.get(Ldap.GID_NUMBER_ATTRIBUTE), false)
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


  private def mapSearchGroupById(name: String) = JEnumerationWrapper {
    retryWithNewContextOnException( () => {
      ctx.search(groupOU,
        String.format("(%s=%s)", Ldap.COMMON_NAME_ATTRIBUTE, name),
        getSimpleSearchControls(Ldap.GID_NUMBER_ATTRIBUTE))
    })
  }

  private def mapSearchPeopleById(name: String) = JEnumerationWrapper {
    retryWithNewContextOnException( () => {
      ctx.search(peopleOU,
        String.format("(%s=%s)", Ldap.USER_ID_ATTRIBUTE, name),
        getSimpleSearchControls(Ldap.UID_NUMBER_ATTRIBUTE))
    })
  }

  def reverseMap(principal: Principal) = {
    new java.util.HashSet[Principal](
      WrapAsJava.asJavaCollection((
        try {
          val id: String = principal.getName
          principal match {
            case _: GidPrincipal =>
              reverseMapSearchGroup(id).map((searchResult) => {
                new GroupNamePrincipal(
                  searchResult.getAttributes.get(Ldap.COMMON_NAME_ATTRIBUTE)
                )
              })
            case _: UidPrincipal =>
              reverseMapSearchUser(id).map((searchResult) => {
                new UserNamePrincipal(
                  searchResult.getAttributes.get(Ldap.USER_ID_ATTRIBUTE)
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
        ).toSet[Principal]) match {
        case mapping if mapping.isEmpty => throw new NoSuchPrincipalException("No reverse mapping")
        case mapping => mapping
      })
  }

  private def reverseMapSearchUser(id: String) = JEnumerationWrapper {
    retryWithNewContextOnException( () => {
      ctx.search(peopleOU,
        new BasicAttributes(Ldap.UID_NUMBER_ATTRIBUTE, id))
    })
  }

  private def reverseMapSearchGroup(id: String) = JEnumerationWrapper {
    retryWithNewContextOnException( () => {
      ctx.search(groupOU,
        new BasicAttributes(Ldap.GID_NUMBER_ATTRIBUTE, id))
    })
  }

  def session(authorizedPrincipals: java.util.Set[Principal], attrib: java.util.Set[AnyRef]) {
    val principal = JSetWrapper(authorizedPrincipals).find(p => p.isInstanceOf[UserNamePrincipal])
    principal match {
      case Some(p) =>
        try {
          val keywords = extractKeywordsFrom(userHome, userRoot)
          val sResult = sessionAttributes(p, keywords:_*)
          sResult.foreach( searchResult => {
            attrib.add(new HomeDirectory(replaceKeywordsByAttributes(userHome, searchResult.getAttributes)))
            attrib.add(new RootDirectory(replaceKeywordsByAttributes(userRoot, searchResult.getAttributes)))
          })
        } catch {
          case e: NamingException => throw new AuthenticationException("no mapping")
        }
      case None => throw new AuthenticationException("no username")
    }
  }

  def replaceKeywordsByAttributes(s: String, attributes: Attributes): String = {
    JEnumerationWrapper(attributes.getAll()).foldLeft(s)((s,attr) => {
      s.replaceAll("%" + attr.getID + "%", attr)})
  }

  private def extractKeywordsFrom(ss: String*) : Seq[String] = {
    ss.flatMap(s => "%([^%]+)%".r.findAllIn(s).matchData.map(_.group(1))).distinct
  }

  private def sessionAttributes(principal: Principal, keywords : String*) = JEnumerationWrapper {
    retryWithNewContextOnException( () => {
      ctx.search(peopleOU,
        String.format(userFilter, principal.getName),
        getSimpleSearchControls(keywords:_*))
    })
  }

  private def getSimpleSearchControls(attr: String*) = {
    val constraints = new SearchControls
    constraints.setSearchScope(SearchControls.SUBTREE_SCOPE)
    constraints.setReturningAttributes(attr.toArray)
    constraints
  }
}
