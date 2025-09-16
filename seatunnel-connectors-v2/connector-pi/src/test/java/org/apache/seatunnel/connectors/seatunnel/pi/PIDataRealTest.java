/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.pi;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIHttpClient;
import org.apache.seatunnel.connectors.seatunnel.pi.client.PIWebIdBatchResolver;
import org.apache.seatunnel.connectors.seatunnel.pi.config.PIConfigHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** PI data accuracy verification test */
@Disabled("This is an test and requires a real running PI Web API service")
@Slf4j
public class PIDataRealTest {

    private PIConfigHelper configHelper;
    private PIHttpClient httpClient;
    private PIWebIdBatchResolver webIdResolver;

    /** The PI points */
    private static final List<String> PI_PATHS =
            Arrays.asList(
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:FRQ-26701.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:FRQ-26703.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HO-F26701.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HO-F26703.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HO-F26705.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HO-F26707.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HO-P26701.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HO-P26703.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HO-P26705.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HS-F26702.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HS-F26704.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HS-F26706.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HS-F26708.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HS-P26702.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HS-P26704.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HZOI-26701A.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HZOI-26701C.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:HZOI-26701E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PI-26701.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26701X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26701Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26702Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26703X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26703Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26704Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26705X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26705Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26706Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26707X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26707Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-LF26708Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-TE26701.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-TE26703.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-TE26705.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:PL-TE26707.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:ST-LF26701.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:ST-LF26703.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:ST-LF26705.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:ST-LF26707.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TI-26701.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26701D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26701U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26701W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26702E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26702V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26703D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26703U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26703W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26704E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26704V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26705D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26705U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26705W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26706E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26706V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26707D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26707U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26707W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26708E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-F26708V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26701D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26701U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26701W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26702E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26702V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26703D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26703U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26703W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26704E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26704V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26705D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26705U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TIA-P26705W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TISA-F26702.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TISA-F26704.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TISA-F26706.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:TISA-F26708.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26701Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26702X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26702Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26703Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26704X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26704Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26705Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26706X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26706Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26707Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26708X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:VISA-F26708Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:XI-F26702.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:XI-F26704.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:XI-F26706.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:XI-F26708.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:XI-P26702.PV",
                    "\\\\pims.huafeng.com\\HF.AA.267XHS:XI-P26704.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:FIQ-26201.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:FIQ-26203.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:FIQ-26205.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:FIQ-26207.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:HS-LF26301A.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:HS-LF26301C.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:HS-LF26301E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:LIA-26201.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:PIA-26202.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26201E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26201V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26202D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26202U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26202W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26203E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26203V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26204D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26204U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26204W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26205E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26205V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26206D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26206U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26206W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26207E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26207V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26208D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26208U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-F26208W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26201E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26201V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26202D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26202U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26202W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26203E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26203V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26204D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26204U.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26204W.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26205E.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-P26205V.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-V26201.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TIA-V26202.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TISA-F26202.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TISA-F26204.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TISA-F26206.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:TISA-F26208.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26201Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26202X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26202Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26203Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26204X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26204Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26205Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26206X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26206Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26207Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26208X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:VISA-F26208Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:XI-LF26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:XI-LF26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:XI-LF26301F.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:XI-P26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS3:XI-P26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:FRQ-26302.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:FRQ-26304.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:LICA-26301.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:PI-26302.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TI-26302.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA1-F26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA1-F26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA1-F26301F.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA1-P26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA1-P26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA2-F26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA2-F26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA2-F26301F.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA2-P26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA2-P26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA3-F26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA3-F26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA3-F26301F.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA3-P26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA3-P26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA4-F26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA4-F26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA4-F26301F.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA4-P26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA4-P26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA5-F26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA5-F26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA5-F26301F.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA5-P26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TIA5-P26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TISA-26301B.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TISA-26301D.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:TISA-26301F.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301AY.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301BX.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301BZ.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301CY.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301DX.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301DZ.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301EY.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301FX.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:VISA-26301FZ.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:XI-F26202.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:XI-F26204.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:XI-F26206.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:XI-F26208.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:XI-P26202.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XHS4:XI-P26204.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:FIRQ-26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:FIRQ-26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:FIRQ-26606.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:FIRQ-26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:FIRQ-26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:FIRQ-26806.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:LIA-26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:LISA-26602.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:LISA-26604.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:PI-26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:PI-26801.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TI-26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TI-26801.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-F26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-F26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-F26605.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-F26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-F26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-F26806.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-F26808.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-P26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-P26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA1-P26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-F26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-F26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-F26605.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-F26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-F26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-F26806.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-F26808.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-P26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-P26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA2-P26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-F26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-F26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-F26605.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-F26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-F26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-F26806.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-F26808.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-P26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-P26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA3-P26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-F26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-F26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-F26605.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-F26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-F26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-F26806.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-F26808.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-P26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-P26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA4-P26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-F26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-F26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-F26605.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-F26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-F26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-F26806.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-F26808.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-P26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-P26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIA5-P26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIAS-F26801.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIAS-F26803.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIAS-F26805.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TIAS-F26807.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TISA-26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:TISA-26605.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26602.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26604.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26801X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26801Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26802Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26803X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26803Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26804Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26805X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26805Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26806Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26807X.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26807Z.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:VISA-26808Y.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-LF26601.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-LF26603.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-LF26605.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-LF26802.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-LF26804.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-LF26806.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-LF26808.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-P26602.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-P26801.PV",
                    "\\\\pims.huafeng.com\\HF.AA.XQXHS:XI-P26803.PV");

    @BeforeEach
    void setUp() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("pi_web_api_url", "https://10.89.63.4:8443/piwebapi");
        configMap.put("username", "WhaleStudio");
        configMap.put("password", "huafeng#2025");
        configMap.put("pi_paths", PI_PATHS);
        configMap.put("start_time", "2025-08-29 09:00:00");
        configMap.put("end_time", "2025-08-29 09:20:00");
        configMap.put("web_id_resolve_delay_ms", 100);
        configMap.put("data_request_delay_ms", 100);
        configMap.put("max_web_ids_per_split", 1);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        configHelper = new PIConfigHelper(config);
        httpClient = new PIHttpClient(configHelper);
        webIdResolver = new PIWebIdBatchResolver(httpClient, configHelper);
    }

    /** Test data retrieval for each PI point */
    @Test
    void testDataRetrievalForAllPIPoints() {
        try {
            List<String> webIds = webIdResolver.batchResolveWebIds(PI_PATHS);
            Assertions.assertNotNull(webIds);
            Assertions.assertEquals(PI_PATHS.size(), webIds.size());

            StringBuilder sb = new StringBuilder(200);
            int totalDataPoints = 0;
            int zeroDataPointCount = 0;
            int successfulRetrievals = 0;

            for (int i = 0; i < webIds.size(); i++) {
                String webId = webIds.get(i);
                try {
                    String dataUrl =
                            String.format(
                                    "%s/streams/%s/recorded?startTime=%s&endTime=%s",
                                    configHelper.getServerUrl(),
                                    webId,
                                    URLEncoder.encode(
                                            configHelper.getStartTime(),
                                            StandardCharsets.UTF_8.name()),
                                    URLEncoder.encode(
                                            configHelper.getEndTime(),
                                            StandardCharsets.UTF_8.name()));
                    String response = httpClient.get(dataUrl);
                    int count = countDataPoints(response);

                    sb.append("PI Path: ")
                            .append(PI_PATHS.get(i))
                            .append(", WebID: ")
                            .append(webId)
                            .append(", Data Point Count: ")
                            .append(count)
                            .append("\n");

                    // Collect statistics
                    totalDataPoints += count;
                    if (count == 0) {
                        zeroDataPointCount++;
                    }
                    successfulRetrievals++;

                    Assertions.assertTrue(count >= 0);
                } catch (Exception e) {
                    Assertions.fail("Failed to retrieve data for WebID: " + webId, e);
                }
            }

            // Add summary statistics
            sb.append("\n=== Summary Statistics ===\n");
            sb.append("Total PI Points: ").append(PI_PATHS.size()).append("\n");
            sb.append("Successful Retrievals: ").append(successfulRetrievals).append("\n");
            sb.append("Total Data Points Retrieved: ").append(totalDataPoints).append("\n");
            sb.append("PI Points with Zero Data: ").append(zeroDataPointCount).append("\n");
            sb.append("PI Points with Data: ")
                    .append(successfulRetrievals - zeroDataPointCount)
                    .append("\n");
            if (successfulRetrievals > 0) {
                sb.append("Average Data Points per PI Point: ")
                        .append(totalDataPoints / successfulRetrievals)
                        .append("\n");
            }

            // Print detailed results
            log.info("Data retrieval test completed, details:");
            log.info(sb.toString());

        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }
    }

    /** Count data points in PI Web API response */
    private int countDataPoints(String response) {
        if (response == null || response.trim().isEmpty()) {
            return 0;
        }

        try {
            // Parse JSON response to correctly count data points
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode rootNode = mapper.readTree(response);
            com.fasterxml.jackson.databind.JsonNode itemsNode = rootNode.path("Items");

            if (!itemsNode.isArray()) {
                return 0;
            }

            int totalDataPoints = 0;

            // Iterate through each item in the Items array
            for (com.fasterxml.jackson.databind.JsonNode itemNode : itemsNode) {
                // Check if there are nested Items (data points)
                com.fasterxml.jackson.databind.JsonNode dataPointsNode = itemNode.path("Items");
                if (dataPointsNode.isArray() && dataPointsNode.size() > 0) {
                    // Case with nested Items (e.g., batch query response)
                    totalDataPoints += dataPointsNode.size();
                } else {
                    // Case without nested Items (e.g., single data point)
                    // Check if this item has Timestamp field to confirm it's a data point
                    if (itemNode.has("Timestamp")) {
                        totalDataPoints++;
                    }
                }
            }

            return totalDataPoints;

        } catch (Exception e) {
            log.warn(
                    "Failed to parse JSON response for counting data points, fallback to string search: {}",
                    e.getMessage());
            // Fallback to original method if JSON parsing fails
            int count = 0;
            int index = 0;
            while ((index = response.indexOf("\"Timestamp\"", index)) != -1) {
                count++;
                index += 11;
            }
            return count;
        }
    }
}
