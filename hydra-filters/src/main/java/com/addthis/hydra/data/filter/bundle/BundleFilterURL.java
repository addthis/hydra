/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.data.filter.bundle;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.basis.collect.HotMap;
import com.addthis.basis.net.NetUtil;
import com.addthis.basis.util.LessBytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.annotations.FieldConfig;

import com.google.common.base.Joiner;
import com.google.common.net.InternetDomainName;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary"> dissects an url and updates
 * the bundle with the component pieces</span>.
 * <p/>
 * <p>
 * URLs can be hard to parse due to the variety of formats and levels of URL encoding they come in.
 * This filter is useful to help 'clean' the URL and to pull common components such as the domain,
 * host, path, and parameters out of the URL string and into individual bundle fields.
 * </p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"url", field:"PAGE_URL", setHost:"PAGE_DOMAIN", clean:true}
 * </pre>
 *
 * @user-reference
 */
public final class BundleFilterURL implements BundleFilter {

    private static final HotMap<String, String> iphost = new HotMap<>(new ConcurrentHashMap<String, String>());
    private static final int maxhostcache = Integer.parseInt(System.getProperty("packet.cachehost.max", "4000"));
    private static final boolean debugMalformed = System.getProperty("path.debug.malformed", "0").equals("1");

    // stolen and modified from NetUtil.resolveDottedIP
    private static String resolveDottedIP(String ip) {
        int ipl = ip.length();
        if (ipl == 0 || !(Character.isDigit(ip.charAt(ipl - 1)) && Character.isDigit(ip.charAt(0)))) {
            return ip;
        }
        try {
            String newhost = InetAddress.getByName(ip).getHostName();
            if (newhost != null) {
                return newhost;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return ip;
    }

    public BundleFilterURL() {
    }

    /**
     * Field containing the URL used as input to this filter.
     */
    @FieldConfig(codable = true, required = true)
    private AutoField field;

    /**
     * If <code>true</code> the URL will be properly url-decoded and a
     * trailing '/' will be added if not already present.
     * The corrected url will be saved back into the input specified by {@link #field}.
     * If {@link #clean} and {@link #fixProto} are both true then
     * result of both transformations are saved back into the input field.
     */
    @FieldConfig(codable = true)
    private boolean clean;

    /**
     * If <code>true</code> then 'http://' will be prepended
     * to the URL if not already present. This transformation
     * will be stored back into the input field if-and-only-if
     * the {@link #clean} parameter is <code>true</code>.
     * Several other parameters such as {@link #setHost}
     * and {@link #setHostNormal} only work on URLs that are
     * prefixed with a protocol. For this reason it can
     * be useful to use this parameter even when {@link #clean}
     * is <code>false</code>.
     */
    @FieldConfig(codable = true)
    private boolean fixProto;

    /**
     * If true the IP of the host identified by the URL will be resolved and set to the
     * returnhost value.
     */
    @FieldConfig(codable = true)
    private boolean resolveIP;

    /**
     * If true the host will be resolved to its base domain. Only affects the field specified
     * by the {@link #setHost setHost} parameter. Cannot be used in conjunction with {@link #toTopPrivateDomain}.
     */
    @FieldConfig(codable = true)
    private boolean toBaseDomain;

    /**
     * If true the host will be resolved to its
     * <a href="https://code.google.com/p/guava-libraries/wiki/InternetDomainNameExplained">
     * top private domain</a>.
     * Only affects the field specified by the {@link #setHost setHost} parameter.
     * Cannot be used in conjunction with {@link #toBaseDomain}.
     */
    @FieldConfig(codable = true)
    private boolean toTopPrivateDomain;

    /**
     * If true then the URL is a file based URL, e.g. file:///.
     */
    @FieldConfig(codable = true)
    private boolean asFile;

    /**
     * Name of the field to populate with the
     * host defined by this URL. If the input URL
     * does not have a protocol prefix then
     * the target field is not populated.
     * To ensure that the input is prefixed with a protocol
     * set the {@link #fixProto} parameter to <code>true</code>.
     */
    @FieldConfig(codable = true)
    private AutoField setHost;

    /**
     * Name of the field to populate with the
     * normalized host defined by this URL.
     * If the input URL does not have a protocol prefix then
     * the target field is not populated.
     * To ensure that the input is prefixed with a protocol
     * set the {@link #fixProto} parameter to <code>true</code>.
     */
    @FieldConfig(codable = true)
    private AutoField setHostNormal;

    /**
     * Name of the field to populate with the top
     * private domain as defined by Google
     * Guava's
     * <a href="http://docs.guava-libraries.googlecode
     * .com/git-history/release/javadoc/com/google/common/net/InternetDomainName
     * .html">InternetDomainName</a> .
     * This field should be used in combination with
     * {@link #fixProto fixProto} set to true.
     */
    @FieldConfig(codable = true)
    private AutoField setTopPrivateDomain;

    /**
     * Name of the field to populate with the path
     * defined by this URL. If null the path will not be set.
     */
    @FieldConfig(codable = true)
    private AutoField setPath;

    /**
     * Name of the field to populate with the parameters
     * defined by this URL. If null the parameters will not be set.
     */
    @FieldConfig(codable = true)
    private AutoField setParams;

    /**
     * Name of the field to populate with the anchor
     * defined by this URL.  If null the anchor will not be set.
     */
    @FieldConfig(codable = true)
    private AutoField setAnchor;

    /**
     * Value to return when input is invalid. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean invalidExit;

    private static final Pattern hostNormalPattern = Pattern.compile("^www*\\d*\\.(.*)");

    public BundleFilterURL setHost(AutoField value) {
        this.setHost = value;
        return this;
    }

    public BundleFilterURL setHostNormal(AutoField value) {
        this.setHostNormal = value;
        return this;
    }

    public BundleFilterURL setField(AutoField value) {
        this.field = value;
        return this;
    }

    public BundleFilterURL setBaseDomain(boolean value) {
        this.toBaseDomain = value;
        return this;
    }

    public BundleFilterURL setTopPrivateDomain(boolean value) {
        this.toTopPrivateDomain = value;
        return this;
    }

    public BundleFilterURL setFixProto(boolean value) {
        this.fixProto = value;
        return this;
    }

    private static final Joiner DOT_JOINER = Joiner.on('.');

    @Override
    public boolean filter(Bundle bundle) {
        String pv = ValueUtil.asNativeString(field.getValue(bundle));
        if (!asFile) {
            if (pv == null || pv.length() < 7) {
                return invalidExit;
            }
            String lpv = pv.trim().toLowerCase();
            if (!(lpv.startsWith("http"))) {
                if (fixProto) {
                    if (clean && lpv.indexOf("%2f") >= 0) {
                        pv = LessBytes.urldecode(pv);
                    }
                    pv = "http://".concat(pv);
                } else {
                    return invalidExit;
                }
            }
            if (clean && (lpv.startsWith("http%") || lpv.startsWith("https%"))) {
                pv = LessBytes.urldecode(pv);
            }
        }
        // up to two 'decoding' passes on the url to try and find a valid one
        for (int i = 0; i < 2; i++) {
            if (pv == null) {
                return invalidExit;
            }
            try {
                URL urec = asFile ? new URL("file://".concat(pv)) : new URL(pv);
                String urlhost = urec.getHost();
                String returnhost = null;
                if (resolveIP) {
                    synchronized (iphost) {
                        returnhost = iphost.get(urlhost).toLowerCase();
                        if (returnhost == null) {
                            returnhost = resolveDottedIP(urlhost);
                            iphost.put(urlhost, returnhost);
                            if (iphost.size() > maxhostcache) {
                                iphost.removeEldest();
                            }
                        }
                    }
                } else {
                    returnhost = urlhost.toLowerCase();
                }
                // store cleaned up (url decoded) version back to packet
                if (clean) {
                    if (urec != null && urec.getPath().isEmpty()) {
                        // if the path element is null, append the slash
                        pv = pv.concat("/");
                    }
                    field.setValue(bundle, ValueFactory.create(pv));
                }
                if (setHost != null) {
                    if (toBaseDomain) {
                        returnhost = NetUtil.getBaseDomain(returnhost);
                    } else if (toTopPrivateDomain) {
                        if (returnhost != null && InternetDomainName.isValid(returnhost)) {
                            InternetDomainName domain = InternetDomainName.from(returnhost);
                            if (domain.hasPublicSuffix()) {
                                InternetDomainName topPrivateDomain = domain.topPrivateDomain();
                                returnhost = topPrivateDomain.toString();
                            }
                        }
                    }
                    setHost.setValue(bundle, ValueFactory.create(returnhost));
                }
                if (setPath != null) {
                    setPath.setValue(bundle, ValueFactory.create(urec.getPath()));
                }
                if (setParams != null) {
                    setParams.setValue(bundle, ValueFactory.create(urec.getQuery()));
                }
                if (setAnchor != null) {
                    setAnchor.setValue(bundle, ValueFactory.create(urec.getRef()));
                }
                if (setHostNormal != null) {
                    Matcher m = hostNormalPattern.matcher(returnhost);
                    if (m.find()) {
                        returnhost = m.group(1);
                    }
                    setHostNormal.setValue(bundle, ValueFactory.create(returnhost));
                }
                if (setTopPrivateDomain != null) {
                    String topDomain = returnhost;
                    if (InternetDomainName.isValid(returnhost)) {
                        InternetDomainName domainName = InternetDomainName.from(returnhost);
                        if (domainName.isTopPrivateDomain() || domainName.isUnderPublicSuffix()) {
                            topDomain = DOT_JOINER.join(domainName.topPrivateDomain().parts());
                        }
                    }
                    setTopPrivateDomain.setValue(bundle, ValueFactory.create(topDomain));
                }
            } catch (MalformedURLException e) {
                if (pv.indexOf("%3") > 0 && pv.indexOf("%2") > 0) {
                    pv = LessBytes.urldecode(pv);
                } else {
                    if (debugMalformed) {
                        System.err.println("malformed(" + i + ") " + pv);
                    }
                    return invalidExit;
                }
            }
        }
        return true;
    }

}
