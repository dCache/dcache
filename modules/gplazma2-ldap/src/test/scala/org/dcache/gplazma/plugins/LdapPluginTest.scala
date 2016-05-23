package org.dcache.gplazma.plugins

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import java.util
import java.util.Properties
import java.security.Principal

import scala.collection.convert.WrapAsJava.setAsJavaSet

import org.dcache.auth.{GroupNamePrincipal, GidPrincipal, UidPrincipal, UserNamePrincipal}
import org.dcache.gplazma.NoSuchPrincipalException
import org.dcache.auth.attributes.{HomeDirectory, RootDirectory}
import org.dcache.ldap4testing.EmbeddedServer;
/**
 * Tests for the gPlazma LDAP plugin.
 *
 * The tests are all ignored by default because they depend on DESY infrastructure.
 */
@RunWith(classOf[JUnitRunner])
class LdapPluginTest extends FlatSpec with Matchers with BeforeAndAfter{

  var server : EmbeddedServer = _
  var ldapPlugin : Ldap = _

  before {
    val initialLdif = ClassLoader.getSystemResourceAsStream("org/dcache/gplazma/plugins/ldap/init.ldif")
    server = new EmbeddedServer(0, initialLdif)
    server.start()

    val pluginProperties = {
        val properties = new Properties
        properties.put(Ldap.LDAP_URL, "ldap://localhost:" + server.getSocketAddress().getPort())
        properties.put(Ldap.LDAP_ORG, "o=dcache,c=org")
        properties.put(Ldap.LDAP_USER_FILTER, "(uid=%s)")
        properties.put(Ldap.LDAP_PEOPLE_TREE, "people")
        properties.put(Ldap.LDAP_GROUP_TREE, "group")
        properties.put(Ldap.LDAP_USER_HOME, "%homeDirectory%")
        properties.put(Ldap.LDAP_USER_ROOT, "/")
        properties.put(Ldap.LDAP_GROUP_MEMBER, "uniqueMember")

        properties.put(Ldap.LDAP_AUTH, "simple")
        properties.put(Ldap.LDAP_BINDDN, "uid=kermit,ou=people,o=dcache,c=org")
        properties.put(Ldap.LDAP_BINDPW, "kermitTheFrog")

        properties
    }

    ldapPlugin = new Ldap(pluginProperties)

  }

  after {
    server.stop()
  }

  "map(Set[Principal])" should "return matching Uid and Gid Principals for an existent user name" in {
    val principals = new util.HashSet[Principal]()
    principals add new UserNamePrincipal("kermit")

    ldapPlugin.map(principals)
    principals.size should be (4)
    principals should contain (new UserNamePrincipal("kermit"))
    principals should contain (new UidPrincipal("1000"))
    principals should contain (new GidPrincipal("1000", true))
    principals should contain (new GidPrincipal("1001", false))
  }

  it should "leave the principals set unchanged for a non existent user name" in {
    val principals = new util.HashSet[Principal]()
    principals add new UserNamePrincipal("bert")

    ldapPlugin.map(principals)
    principals should have size 1
    principals should contain (new UserNamePrincipal("bert"))
  }

  "map(UserNamePrincipal)" should "return a UidPrincipal for an existing user name" in {
    ldapPlugin.map(new UserNamePrincipal("kermit")) should be (new UidPrincipal("1000"))
  }

  it should "throw a NoSuchPrincipalException if a user does not exist" in {

    intercept[NoSuchPrincipalException] {
      ldapPlugin.map(new UserNamePrincipal("bert"))
    }
  }

  "reverseMap" should "return a Set containing a UserNamePrincipal for an existing Uid" in {
    ldapPlugin.reverseMap(new UidPrincipal("1000")) should contain (new UserNamePrincipal("kermit"))
  }

  it should "return a serializable Set" in {
    val set = ldapPlugin.reverseMap(new UidPrincipal("1000"))
    set.isInstanceOf[java.io.Serializable] should be (true)
  }

  it should "throw an NoSuchPrincipalException for a non existent Uid" in {

    intercept[NoSuchPrincipalException] {
      ldapPlugin.reverseMap(new UidPrincipal("666"))
    }
  }

  it should "return a Set containing a GroupNamePrincipal for an existing Gid" in {
    ldapPlugin.reverseMap(new GidPrincipal("1001", true)) should contain (new GroupNamePrincipal("actor"))
  }

  it should "throw a NoSuchPrincipalException for a non existent Gid" in {

    intercept[NoSuchPrincipalException] {
      ldapPlugin.reverseMap(new GidPrincipal("1002", true))
    }
  }

  "session" should "return the user's home and root directory, and the access rights" in {
    val attr = new java.util.HashSet[AnyRef]()
    ldapPlugin.session(setAsJavaSet(Set[Principal](new UserNamePrincipal("bernd"))), attr)

    attr should have size 2
    attr should contain (new HomeDirectory("/home/bernd"))
    attr should contain (new RootDirectory("/"))
  }

}
