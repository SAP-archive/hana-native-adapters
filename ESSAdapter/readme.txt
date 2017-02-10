/*
 * $Header: SAP HANA EIM Demonstration Purpose Adapter $
 * $Author: Andong.Li $
 * $Revision: 1.0 $
 * $Date: 2017/02/09 $
 *
 * ====================================================================
 *
 * Copyright (C) 2001-2017 by copyrights@sap.com
 *
 * This program is designed to show you how to write SAP HANA EIM Adapters  
 * to be used by SAP ESS(Enterprise Semantic Search) Engine. As a part of 
 * SDI, adapters are being appointed to extract Remote Source Objects 
 * Metadata, sending back as dictionary, and used by ESS Search Engine.
 *
 * API functions related to ESS Search Engine:
 * 1. public List<BrowseNode> browseMetadata() throws AdapterException
 * 2. public Metadata importMetadata(String nodeId) throws AdapterException
 * 3. public List<BrowseNode> loadTableDictionary(String lastUniqueName) 
 *     throws AdapterException
 * 4. public void setNodesListFilter(RemoteObjectsFilter remoteObjectsFilter
 *
 * Please refer to ESSAdapter.java to get a brief understanding of how these
 * function works to provide ESS information.
 *
 */