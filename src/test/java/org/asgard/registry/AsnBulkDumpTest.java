package org.asgard.registry;

import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class AsnBulkDumpTest {

    @Test
    void parsesDelegatedExtendedAndExpandsRange() throws Exception {
        final Path tmp = Files.createTempFile("delegated", ".txt");
        Files.writeString(tmp,
                "# header\n" +
                        "arin|US|asn|64512|3|19960101|allocated|x\n" +
                        "arin|US|ipv4|1.2.3.0|256|19960101|allocated|x\n",
                StandardCharsets.UTF_8);

        final Map<Long, java.util.List<AsnBulkDump.DelegatedRecord>> m = AsnBulkDump.parseDelegatedExtended(tmp);
        assertThat(m).containsKeys(64512L, 64513L, 64514L);
        assertThat(m.get(64512L).get(0).raw()).contains("arin|US|asn|64512|3|19960101|allocated");
    }

    @Test
    void parsesRpslAutNumAndContinuationLines() throws Exception {
        final String rpsl =
                "aut-num: AS64512\n" +
                        "as-name: TEST-AS\n" +
                        "descr: line1\n" +
                        " descr cont\n" +
                        "org: ORG-TEST1\n" +
                        "admin-c: AA1-TEST\n" +
                        "\n";
        final Path gz = Files.createTempFile("autnum", ".gz");
        try (OutputStream out = new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(gz)))) {
            out.write(rpsl.getBytes(StandardCharsets.UTF_8));
        }

        final var objs = AsnBulkDump.iterRpslObjectsFromGz(gz);
        assertThat(objs).hasSize(1);
        final var o = objs.get(0);
        assertThat(o.objType()).isEqualTo("aut-num");
        assertThat(o.attrs().get("descr").get(0)).contains("line1").contains("descr cont");
        assertThat(o.attrs().get("org").get(0)).isEqualTo("ORG-TEST1");
    }

    @Test
    void parsesArinBulkZipXml() throws Exception {
        final String xml =
                "<arin_db>" +
                        "  <org><handle>ORG-EXAMPLE</handle><name>Example Org</name></org>" +
                        "  <asn><startAsNumber>64512</startAsNumber><orgHandle>ORG-EXAMPLE</orgHandle></asn>" +
                        "</arin_db>";

        final Path zip = Files.createTempFile("arin", ".zip");
        try (ZipOutputStream z = new ZipOutputStream(Files.newOutputStream(zip))) {
            final ZipEntry e = new ZipEntry("arin_db.xml");
            z.putNextEntry(e);
            z.write(xml.getBytes(StandardCharsets.UTF_8));
            z.closeEntry();
        }

        final var res = AsnBulkDump.parseArinBulk(zip);
        assertThat(res.asns()).containsKey(64512L);
        assertThat(res.orgs()).containsKey("ORG-EXAMPLE");
        assertThat(res.asns().get(64512L).get("orghandle")).isEqualTo("ORG-EXAMPLE");
        assertThat(res.orgs().get("ORG-EXAMPLE").get("name")).isEqualTo("Example Org");
    }

    @Test
    void parsesArinBulkZipExpandsAsnRange() throws Exception {
        final String xml =
                "<arin_db>" +
                        "  <org><handle>7ESG</handle><name>Air Force Systems Networking</name></org>" +
                        "  <asn><startAsNumber>727</startAsNumber><endAsNumber>746</endAsNumber>" +
                        "<name>AFCONC-BLOCK2-AS</name><orgHandle>7ESG</orgHandle></asn>" +
                        "</arin_db>";

        final Path zip = Files.createTempFile("arin", ".zip");
        try (ZipOutputStream z = new ZipOutputStream(Files.newOutputStream(zip))) {
            z.putNextEntry(new ZipEntry("arin_db.xml"));
            z.write(xml.getBytes(StandardCharsets.UTF_8));
            z.closeEntry();
        }

        final var res = AsnBulkDump.parseArinBulk(zip);
        // All ASNs in range 727-746 should be present
        assertThat(res.asns()).containsKeys(727L, 728L, 740L, 746L);
        assertThat(res.asns()).doesNotContainKey(747L);
        // Each ASN should have the name and orgHandle
        assertThat(res.asns().get(728L).get("name")).isEqualTo("AFCONC-BLOCK2-AS");
        assertThat(res.asns().get(728L).get("orghandle")).isEqualTo("7ESG");
        assertThat(res.orgs().get("7ESG").get("name")).isEqualTo("Air Force Systems Networking");
    }

    @Test
    void assembleMetadataUsesArinBulkNameWhenRpslMissing() throws Exception {
        final var delegated = Map.of(728L, java.util.List.of(
                new AsnBulkDump.DelegatedRecord(727, 20, "arin", "US", "assigned", "00000000", "raw")));
        final Map<Long, AsnBulkDump.RpslObject> rpslByAsn = Map.of(); // No RPSL data
        final var arinData = new AsnBulkDump.ArinBulkData(
                Map.of(728L, Map.of("name", "AFCONC-BLOCK2-AS", "orghandle", "7ESG")),
                Map.of("7ESG", Map.of("name", "Air Force Systems Networking")));

        final var results = AsnBulkDump.assembleMetadata(delegated, rpslByAsn, arinData);

        assertThat(results).hasSize(1);
        final var metadata = results.get(0);
        assertThat(metadata.asn()).isEqualTo(728L);
        assertThat(metadata.name()).isEqualTo("AFCONC-BLOCK2-AS");
        assertThat(metadata.registrant().organization()).isEqualTo("Air Force Systems Networking");
    }
}
