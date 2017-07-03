*** Settings ***
Resource          ../resources.robot
Default Tags      Suppress WWW-Authenticate    Authentication

*** Variables ***
${StatusCode}         401


*** Test Cases ***
Response Header Include WWW-Authenticate
    ${RequestHeaders}=                   Create Dictionary             Accept=application\/json
    &{ExpectedResponseHeader}=           Create Dictionary             include=WWW-Authenticate
    ...                                  exclude=Suppress-WWW-Authenticate
    ${Site1}=                            Set Up A Site Entry           ${HTTP}
    ...                                  /private?children=true        ${ExpectedResponseHeader}     ${RequestHeaders}
    ${Site2}=                            Set Up A Site Entry           ${HTTPS}
    ...                                  /private?children=true        ${ExpectedResponseHeader}     ${RequestHeaders}
    ${Site3}=                            Set Up A Site Entry           ${HTTPS_NA}                   /
    ...                                  ${ExpectedResponseHeader}     ${RequestHeaders}
    @{List Of Sites}=   Create List      ${Site1}                      ${Site2}                      ${Site3}
    Send Request to All Listed Sites     @{List Of Sites}

*** Test Cases ***
Response Header Include Suppress-WWW-Authenticate but No WWW-Authenticate
    ${RequestHeaders}=                   Create Dictionary             Accept=application\/json
    ...                                  Suppress-WWW-Authenticate=Suppress
    &{ExpectedResponseHeader}=           Create Dictionary             include=Suppress-WWW-Authenticate
    ...                                  exclude=WWW-Authenticate
    ${Site1}=                            Set Up A Site Entry           ${HTTP}
    ...                                  /private?children=true        ${ExpectedResponseHeader}     ${RequestHeaders}
    ${Site2}=                            Set Up A Site Entry           ${HTTPS}
    ...                                  /private?children=true        ${ExpectedResponseHeader}     ${RequestHeaders}
    ${Site3}=                            Set Up A Site Entry           ${HTTPS_NA}                   /
    ...                                  ${ExpectedResponseHeader}     ${RequestHeaders}
    @{List Of Sites}=   Create List      ${Site1}                      ${Site2}                      ${Site3}
    Send Request to All Listed Sites     @{List Of Sites}

*** Keywords ***
Send Request to All Listed Sites
    [Arguments]    @{SiteInformation}
    :FOR           ${entry}                      IN                     @{SiteInformation}
    \              ${SiteName}=                  Get From Dictionary    ${entry}                SiteName
    \              ${Path}=                      Get From Dictionary    ${entry}                Path
    \              ${ExpectedResponseHeader}=    Get From Dictionary    ${entry}                ExpectedResponseHeader
    \              ${RequestHeaders}=            Get From Dictionary    ${entry}                RequestHeaders
    \              Create Session                rest                   ${SiteName}
    \              ${resp}=                      Get Request            rest                    ${Path}
    ...            headers=${RequestHeaders}
    \              Should Be Equal As Strings    ${resp.status_code}    ${StatusCode}
    \              Should Not Contain            ${resp.headers}        ${ExpectedResponseHeader.exclude}
    ...            case_insensitive=False
    \              Should Contain                ${resp.headers}        ${ExpectedResponseHeader.include}
    ...            case_insensitive=False

Set Up A Site Entry
    [Arguments]    ${SiteName}                   ${Path}                ${ExpectedResponseHeader}
    ...            ${RequestHeaders}
    ${site}=       Create Dictionary             SiteName=${SiteName}   Path=${Path}
    ...            RequestHeaders=${RequestHeaders}                     ExpectedResponseHeader=${ExpectedResponseHeader}
    [return]       ${site}