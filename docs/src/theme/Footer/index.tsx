import React from 'react';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import clsx from 'clsx';
import { FaBlog, FaGithub, FaDiscord, FaTwitter, FaLinkedin, FaYoutube } from 'react-icons/fa';
import { CgFileDocument } from 'react-icons/cg';
import styles from './styles.module.css';

/**
 * Custom Footer component matching cocoindex.io footer design
 */
export default function FooterWrapper(): React.ReactElement {
    const currentYear = new Date().getFullYear();

    return (
        <footer className={clsx('footer', styles.footerCustom)}>
            <div className={clsx('container', styles.footerContainer)}>
                <div className={styles.footerContent}>
                    {/* Resources Column */}
                    <div className={styles.footerColumn}>
                        <h3 className={styles.footerTitle}>Resources</h3>
                        <div className={styles.footerLinks}>
                            <Link
                                href="https://cocoindex.io/blogs/"
                                className={styles.footerLink}
                            >
                                <FaBlog className={styles.footerIcon} />
                                Blog
                            </Link>
                            <Link
                                href="https://cocoindex.io/docs/"
                                className={styles.footerLink}
                            >
                                <CgFileDocument className={styles.footerIcon} />
                                Documentation
                            </Link>
                            <Link
                                href="https://www.youtube.com/@cocoindex-io"
                                className={styles.footerLink}
                            >
                                <FaYoutube className={styles.footerIcon} />
                                YouTube
                            </Link>
                        </div>
                    </div>

                    {/* Community Column */}
                    <div className={styles.footerColumn}>
                        <h3 className={styles.footerTitle}>Community</h3>
                        <div className={styles.footerLinks}>
                            <Link
                                href="https://github.com/cocoindex-io/cocoindex"
                                className={styles.footerLink}
                            >
                                <FaGithub className={styles.footerIcon} />
                                GitHub
                            </Link>
                            <Link
                                href="https://discord.gg/zpA9S2DR7s"
                                className={styles.footerLink}
                            >
                                <FaDiscord className={styles.footerIcon} />
                                Discord
                            </Link>
                            <Link
                                href="https://twitter.com/cocoindex_io"
                                className={styles.footerLink}
                            >
                                <FaTwitter className={styles.footerIcon} />
                                Twitter
                            </Link>
                            <Link
                                href="https://www.linkedin.com/company/cocoindex/about/"
                                className={styles.footerLink}
                            >
                                <FaLinkedin className={styles.footerIcon} />
                                LinkedIn
                            </Link>
                        </div>
                    </div>

                    {/* Company Column */}
                    <div className={styles.footerColumn}>
                        <h3 className={styles.footerTitle}>Company</h3>
                        <div className={styles.footerLinks}>
                            <Link
                                href="https://cocoindex.io/privacy-policy"
                                className={styles.footerLink}
                            >
                                Privacy Policy
                            </Link>
                            <Link
                                href="https://cocoindex.io/terms-of-use"
                                className={styles.footerLink}
                            >
                                Terms of Use
                            </Link>
                        </div>
                    </div>

                    {/* Logo and Copyright Section */}
                    <div className={styles.footerLogoSection}>
                        <div className={styles.logoContainer}>
                            <img
                                src={useBaseUrl('img/icon.svg')}
                                alt="CocoIndex"
                                className={styles.logo}
                            />
                        </div>
                        <div className={styles.copyrightSection}>
                            <p className={styles.copyrightText}>
                                © {currentYear} CocoIndex
                            </p>
                            <Link
                                href="mailto:hi@cocoindex.io"
                                className={styles.emailText}
                            >
                                hi@cocoindex.io
                            </Link>
                        </div>
                    </div>
                </div>
            </div>
        </footer>
    );
}


