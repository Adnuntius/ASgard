package org.asgard;

import java.util.List;

public final class Taxonomy {
    private Taxonomy() {
    }

    public static List<String> categories() {
        return List.of(
                "VPN",
                "Hosting",
                "ISP",
                "Enterprise",
                "Infrastructure"
        );
    }

    public static String prompt() {
        return """
                Classify the Autonomous System into exactly one category:
                -VPN: VPN and anonymization providers including exit-nodes
                -Hosting: Datacenters,cloud providers,VPS/bare-metal/colo/CDN
                -ISP: Residential ISPs,mobile carriers,cable/fiber operators,last-mile providers
                -Enterprise: Internal networks for corporations,government,military,universities,research,education
                -Infrastructure: Large carriers,Tier-1s,IXPs,route-servers""";
    }
}
