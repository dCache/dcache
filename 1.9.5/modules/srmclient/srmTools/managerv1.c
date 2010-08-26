#include "soapH.h"
#include "soapSRMServerV1.nsmap"
main()
{
	struct soap soap;
	soap_init(&soap);


	if (soap_call_tns__ping ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "ping",/* tns__pingResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__getEstPutTime ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "getEstPutTime",/* ArrayOfstring * arg0, ArrayOfstring * arg1, ArrayOflong * arg2, ArrayOfboolean * arg3, ArrayOfstring * arg4, tns__getEstPutTimeResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__put ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "put",/* ArrayOfstring * arg0, ArrayOfstring * arg1, ArrayOflong * arg2, ArrayOfboolean * arg3, ArrayOfstring * arg4, tns__putResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__getRequestStatus ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "getRequestStatus",/* xsd__int  arg0, tns__getRequestStatusResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__copy ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "copy",/* ArrayOfstring * arg0, ArrayOfstring * arg1, ArrayOfboolean * arg2, tns__copyResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__getEstGetTime ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "getEstGetTime",/* ArrayOfstring * arg0, ArrayOfstring * arg1, tns__getEstGetTimeResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__getProtocols ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "getProtocols",/* tns__getProtocolsResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__advisoryDelete ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "advisoryDelete",/* ArrayOfstring * arg0, tns__advisoryDeleteResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__pin ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "pin",/* ArrayOfstring * arg0, tns__pinResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__unPin ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "unPin",/* ArrayOfstring * arg0, xsd__int  arg1, tns__unPinResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__setFileStatus ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "setFileStatus",/* xsd__int  arg0, xsd__int  arg1, xsd__string  arg2, tns__setFileStatusResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__mkPermanent ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "mkPermanent",/* ArrayOfstring * arg0, tns__mkPermanentResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__get ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "get",/* ArrayOfstring * arg0, ArrayOfstring * arg1, tns__getResponse * out*/ ))
		soap_print_fault(&soap,stderr);


	if (soap_call_tns__getFileMetaData ( &soap, "http://stkendca3a.fnal.gov:24128/srm/managerv1", "getFileMetaData",/* ArrayOfstring * arg0, tns__getFileMetaDataResponse * out*/ ))
		soap_print_fault(&soap,stderr);


}
