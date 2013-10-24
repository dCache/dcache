package org.dcache.gplazma.plugins

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scala.collection.JavaConversions._

import java.util.Properties

import org.dcache.auth.{GroupNamePrincipal, GidPrincipal, UidPrincipal, UserNamePrincipal}
import org.dcache.gplazma.NoSuchPrincipalException
import java.security.Principal
import org.dcache.auth.attributes.{ReadOnly, HomeDirectory, RootDirectory}
import java.util

/**
 * Tests for the gPlazma LDAP plugin.
 *
 * The tests are all ignored by default because they depend on DESY infrastructure.
 */
@RunWith(classOf[JUnitRunner])
@Ignore
class LdapPluginTest extends FlatSpec with Matchers {

  val pluginProperties = {
    val properties = new Properties
    properties.put(Ldap.LDAP_SERVER, "wof-dav.desy.de")
    properties.put(Ldap.LDAP_PORT, "389")
    properties.put(Ldap.LDAP_ORG, "ou=NIS,o=DESY,c=DE")
    properties.put(Ldap.LDAP_USER_FILTER, "(uid=%s)")
    properties.put(Ldap.LDAP_PEOPLE_TREE, "People")
    properties.put(Ldap.LDAP_GROUP_TREE, "Groups")
    properties
  }

  def ldapPlugin = new Ldap(pluginProperties)

  "map(Set[Principal])" should "return matching Uid and Gid Principals for an existent user name" in {
    val principals = new util.HashSet[Principal]()
    principals add new UserNamePrincipal("karsten")

    ldapPlugin.map(principals)
    principals.size should be (4)
    principals should contain (new UserNamePrincipal("karsten"))
    principals should contain (new UidPrincipal("121"))
    principals should contain (new GidPrincipal("3752", true))
    principals should contain (new GidPrincipal("1000", false))
  }

  it should "leave the principals set unchanged for a non existent user name" in {
    val principals = new util.HashSet[Principal]()
    principals add new UserNamePrincipal("bert")

    ldapPlugin.map(principals)
    principals should have size 1
    principals should contain (new UserNamePrincipal("bert"))
  }

  "map(UserNamePrincipal)" should "return a UidPrincipal for an existing user name" in {
    ldapPlugin.map(new UserNamePrincipal("karsten")) should be (new UidPrincipal("121"))
  }

  it should "throw a NoSuchPrincipalException if a user does not exist" in {

    intercept[NoSuchPrincipalException] {
      ldapPlugin.map(new UserNamePrincipal("bert"))
    }
  }

  "reverseMap" should "return a Set containing a UserNamePrincipal for an existing Uid" in {
    ldapPlugin.reverseMap(new UidPrincipal("121")) should contain (new UserNamePrincipal("karsten"))
  }

  it should "return an empty Set for an non existent Uid" in {
    ldapPlugin.reverseMap(new UidPrincipal("666")) should be ('empty)
  }

  it should "return a Set containing a GroupNamePrincipal for an existing Gid" in {
    ldapPlugin.reverseMap(new GidPrincipal("3752", true)) should contain (new GroupNamePrincipal("htw-berlin"))
  }

  it should "return an empty Set for a non existent Gid" in {
    ldapPlugin.reverseMap(new GidPrincipal("666", true)) should be ('empty)
  }

  "session" should "return the user's home and root directory, and the access rights" in {
    var attr = new java.util.HashSet[AnyRef]()
    ldapPlugin.session(Set[Principal](new UserNamePrincipal("karsten")), attr)

    attr should have size 3
    attr should contain (new RootDirectory("/"))
    attr should contain (new HomeDirectory("/dcache-cloud/karsten"))
    attr should contain (new ReadOnly(false))
  }

}