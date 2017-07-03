*** Settings ***
Resource          ../resources.robot
Suite Setup       Run Keywords
...               Get Username    AND
...               Get Password    AND
...               Get Basic Authentication Credential
Default Tags      Basic-Authentication    Authentication

*** Test Cases ***
Wrong Username And Password
    ${wrong_auth}=      Create List             nousername          nopassword
    ${Site1}=           Set Up A Site Entry     ${HTTP}             /private?children=true    ${wrong_auth}         401
    ${Site2}=           Set Up A Site Entry     ${HTTPS}            /private?children=true    ${wrong_auth}         401
    ${Site3}=           Set Up A Site Entry     ${HTTPS_NA}         /                         ${wrong_auth}         401
    @{List Of Sites}=   Create List             ${Site1}            ${Site2}                  ${Site3}
    Send Request to All Listed Sites            @{List Of Sites}

*** Test Cases ***
No Username And Password
    ${Site1}=           Set Up A Site Entry     ${HTTP}             /private?children=true    ${EMPTY}              401
    ${Site2}=           Set Up A Site Entry     ${HTTPS}            /private?children=true    ${EMPTY}              401
    ${Site3}=           Set Up A Site Entry     ${HTTPS_NA}         /                         ${EMPTY}              401
    @{List Of Sites}=   Create List             ${Site1}            ${Site2}                  ${Site3}
    Send Request to All Listed Sites            @{List Of Sites}

*** Test Cases ***
Empty Password
    ${empty_pw_auth}=   Create List             nousername          ${EMPTY}
    ${Site1}=           Set Up A Site Entry     ${HTTP}             /private?children=true    ${empty_pw_auth}      401
    ${Site2}=           Set Up A Site Entry     ${HTTPS}            /private?children=true    ${empty_pw_auth}      401
    ${Site3}=           Set Up A Site Entry     ${HTTPS_NA}         /                         ${empty_pw_auth}      401
    @{List Of Sites}=   Create List             ${Site1}            ${Site2}                  ${Site3}
    Send Request to All Listed Sites            @{List Of Sites}


*** Test Cases ***
Empty Username
    ${empty_un_auth}=   Create List             ${EMPTY}            nopassword
    ${Site1}=           Set Up A Site Entry     ${HTTP}             /private?children=true    ${empty_un_auth}      401
    ${Site2}=           Set Up A Site Entry     ${HTTPS}            /private?children=true    ${empty_un_auth}      401
    ${Site3}=           Set Up A Site Entry     ${HTTPS_NA}         /                         ${empty_un_auth}      401
    @{List Of Sites}=   Create List             ${Site1}            ${Site2}                  ${Site3}
    Send Request to All Listed Sites            @{List Of Sites}

*** Test Cases ***
Valid Username and Password
    ${Site1}=           Set Up A Site Entry     ${HTTP}             /private?children=true    ${auth}               200
    ${Site2}=           Set Up A Site Entry     ${HTTPS}            /private?children=true    ${auth}               200
    ${Site3}=           Set Up A Site Entry     ${HTTPS_NA}         /                         ${auth}               200
    @{List Of Sites}=   Create List             ${Site1}            ${Site2}                  ${Site3}
    Send Request to All Listed Sites            @{List Of Sites}


*** Keywords ***
Get Basic Authentication Credential
    ${auth}=                                    Create List         ${username}               ${password}
    Set Suite Variable                          ${auth}


Send Request to All Listed Sites
    [Arguments]         @{SiteInformation}
    :FOR                ${entry}                IN                   @{SiteInformation}
    \                   ${SiteName}=            Get From Dictionary  ${entry}                SiteName
    \                   ${Path}=                Get From Dictionary  ${entry}                Path
    \                   ${AuthValue}=           Get From Dictionary  ${entry}                AuthValue
    \                   ${ExceptedStatusCode}=  Get From Dictionary  ${entry}                ExceptedStatusCode
    \                   Create Session          rest                 ${SiteName}             auth=${AuthValue}
    \                   ${resp}=                Get Request          rest                    ${Path}
    \                   Should Be Equal As Strings                   ${resp.status_code}     ${ExceptedStatusCode}

Set Up A Site Entry
    [Arguments]         ${SiteName}             ${Path}              ${AuthValue}            ${Code}
    ${site}=            Create Dictionary       Path=${Path}         AuthValue=${AuthValue}  SiteName=${SiteName}
    ...                 ExceptedStatusCode=${Code}
    [return]            ${site}