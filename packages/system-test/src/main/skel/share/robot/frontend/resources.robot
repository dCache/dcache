*** Settings ***
Library           Collections
Library           RequestsLibrary
Library           Process

*** Variables ***
${SERVER}         localhost
${ENDPOINT}       api/v1/namespace
${HTTP}           http://${SERVER}:3880/${ENDPOINT}
${HTTPS}          https://${SERVER}:3881/${ENDPOINT}
${HTTPS_NA}       https://${SERVER}:3882/${ENDPOINT}

*** Keywords ***
Run WhoAmI Command
    ${whoamI}=                Run Process                   whoami           shell=true
    [return]                  ${whoamI.stdout}

Get Username
    ${whoamI} =               Run WhoAmI Command
    ${username} =             Get Variable Value            ${username}      ${whoamI}
    Set Suite Variable        ${username}

Get Password
    ${password} =             Get Variable Value            ${password}      password
    Set Suite Variable        ${password}