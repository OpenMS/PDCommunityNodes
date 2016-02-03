using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Xml;
using System.Xml.Linq;
using System.IO;
using System.Web.UI;
using System.Text.RegularExpressions;

using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.BL.Processing;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.Exceptions;
using Thermo.Magellan.Utilities;

namespace PD.OpenMS.AdapterNodes
{
    /// <summary>
    /// Set of delegates needed by methods in OpenMSCommons in order to be able to use the logging / messaging functionality of the nodes they're called from (HACK).
    /// </summary>
    public struct NodeDelegates
    {
        public delegate void NodeLoggerWarningDelegate(string s, params object[] args);
        public delegate Exception NodeLoggerErrorDelegate(Exception e, string s, params object[] args);
        public delegate void SendAndLogTemporaryMessageDelegate(string s, bool writeToLog = true);
        public delegate void SendAndLogMessageDelegate(string s, bool writeToLog = true);
        public delegate void SendAndLogErrorMessageDelegate(string s, bool writeToLog = true);
        public delegate void WriteLogMessageDelegate(MessageLevel ml, string s);

        public NodeLoggerWarningDelegate warnLog;
        public NodeLoggerErrorDelegate errorLog;
        public SendAndLogTemporaryMessageDelegate logTmpMessage;
        public SendAndLogMessageDelegate logMessage;
        public SendAndLogErrorMessageDelegate errorLogMessage;
        public WriteLogMessageDelegate writeLogMessage;
    }

    /// <summary>
    /// Methods that are shared between several nodes. Cannot be done by inheritance (I think), as multiple inheritance isn't allowed.
    /// </summary>
    public class OpenMSCommons
    {
        /// <summary>
        /// Create a default INI file for the given TOPP tool
        /// </summary>
        public static void CreateDefaultINI(string exec_path, string ini_path, string scratch_dir, NodeDelegates nd)
        {
            var timer = Stopwatch.StartNew();

            var data_path = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(exec_path), @"../share/OpenMS"));
            var process_startinfo = new ProcessStartInfo();
            process_startinfo.EnvironmentVariables["OPENMS_DATA_PATH"] = data_path;
            process_startinfo.FileName = exec_path;
            process_startinfo.WorkingDirectory = scratch_dir;
            process_startinfo.Arguments = " -write_ini " + String.Format("\"{0}\"", ini_path);
            process_startinfo.UseShellExecute = false;
            process_startinfo.RedirectStandardOutput = false;
            process_startinfo.CreateNoWindow = false;

            var process = new Process
            {
                StartInfo = process_startinfo
            };

            try
            {
                process.Start();

                try
                {
                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                    throw;
                }

                if (process.ExitCode != 0)
                {
                    throw new MagellanProcessingException(
                        String.Format(
                            "The exit code of {0} was {1}. (The expected exit code is 0)",
                            Path.GetFileName(process.StartInfo.FileName), process.ExitCode));
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                throw;
            }
            catch (Exception ex)
            {
                nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    nd.warnLog("The process [{0}] hasn't finished correctly -> force to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }
        }

        /// <summary>
        /// Write a set of parameters to the specified OpenMS INI file
        /// </summary>
        public static void WriteParamsToINI(string ini_path, Dictionary<string, string> parameters)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                foreach (string param in parameters.Keys)
                {
                    if (item.Attributes["name"].Value == param)
                    {
                        item.Attributes["value"].Value = parameters[param];
                    }
                }
            }
            doc.Save(ini_path);
        }

        /// <summary>
        /// Write a nested parameter to the specified OpenMS INI file
        /// </summary>
        public static void WriteNestedParamToINI(string ini_path, Triplet parameters)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                if ((item.ParentNode.Attributes["name"].Value == parameters.First.ToString()) && (item.Attributes["name"].Value == parameters.Second.ToString()))
                {
                    item.Attributes["value"].Value = parameters.Third.ToString();
                }
            }
            doc.Save(ini_path);
        }

        /// <summary>
        /// Write an item list parameter to an OpenMS INI file
        /// </summary>
        public static void WriteItemListToINI(string[] vars, string ini_path, string mode, bool clear_list_first = false)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList itemlist = doc.GetElementsByTagName("ITEMLIST");
            foreach (XmlElement item in itemlist)
            {
                //mode: in or out?
                if (item.Attributes["name"].Value == mode)
                {
                    if (clear_list_first)
                    {
                        item.IsEmpty = true;
                    }

                    foreach (var fn in vars)
                    {
                        //We add LISTITEMS to ITEMLISTS
                        var listitem = doc.CreateElement("LISTITEM");
                        XmlAttribute newAttribute = doc.CreateAttribute("value");
                        newAttribute.Value = fn;
                        listitem.SetAttributeNode(newAttribute);
                        item.AppendChild(listitem);
                    }
                }
            }
            doc.Save(ini_path);
        }

        /// <summary>
        /// Write mz and rt parameters for MapAligner or FeatureLinker. Different function than WriteParamsToINI due to specific structure in considered tools
        /// </summary>
        public static void WriteThresholdsToINI(MassToleranceParameter mz_threshold, DoubleParameter rt_threshold, string ini_path)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "max_difference"))
                {
                    var mzthresh = mz_threshold.Value.Tolerance.ToString();
                    item.Attributes["value"].Value = mzthresh;
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "unit"))
                {
                    //always use ppm
                    item.Attributes["value"].Value = "ppm";
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_RT") && (item.Attributes["name"].Value == "max_difference"))
                {
                    item.Attributes["value"].Value = (rt_threshold.Value * 60).ToString(); //need to convert minute(PD) to seconds(OpenMS)!
                }
            }
            doc.Save(ini_path);
        }

        /// <summary>
        /// Run TOPP tool. Parameters are passed via the OpenMS INI file at param_path
        /// </summary>        
        public static void RunTOPPTool(string exec_path, string param_path, string scratch_dir, NodeDelegates nd)
        {
            var timer = Stopwatch.StartNew();

            var data_path = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(exec_path), @"../share/OpenMS"));
            var process_startinfo = new ProcessStartInfo();
            process_startinfo.EnvironmentVariables["OPENMS_DATA_PATH"] = data_path;
            process_startinfo.FileName = exec_path;
            process_startinfo.WorkingDirectory = scratch_dir;
            process_startinfo.Arguments = " -ini " + String.Format("\"{0}\"", param_path);
            process_startinfo.UseShellExecute = false;
            process_startinfo.RedirectStandardOutput = true;
            process_startinfo.CreateNoWindow = false;

            var process = new Process
            {
                StartInfo = process_startinfo
            };

            nd.logTmpMessage(String.Format("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                                        process.StartInfo.FileName,
                                        process.StartInfo.WorkingDirectory,
                                        process.StartInfo.Arguments));

            nd.writeLogMessage(MessageLevel.Debug,
                            String.Format("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                                          process.StartInfo.FileName,
                                          process.StartInfo.WorkingDirectory,
                                          process.StartInfo.Arguments));

            try
            {
                process.Start();

                try
                {
                    string current_work = "";
                    while (process.HasExited == false)
                    {
                        var output = process.StandardOutput.ReadLine();

                        // move on if no new announcement. 
                        if (String.IsNullOrEmpty(output))
                        {
                            continue;
                        }

                        //store all results (for now?) of OpenMS Tool output
                        nd.writeLogMessage(MessageLevel.Debug, output);

                        // Parse the output and report progress using the method SendAndLogTemporaryMessage
                        if (output.Contains(@"Progress of 'loading mzML file':"))
                        {
                            current_work = "Progress of 'loading mzML file':";
                        }
                        else if (output.Contains("Progress of 'loading chromatograms':"))
                        {
                            current_work = "Progress of 'loading chromatograms':";
                        }
                        else if (output.Contains("Progress of 'Aligning input maps':"))
                        {
                            current_work = "Progress of 'Aligning input maps':";
                        }
                        else if (output.Contains("Progress of 'linking features':"))
                        {
                            current_work = "Progress of 'linking features':";
                        }
                        else if (output.Contains("%"))
                        {
                            nd.logTmpMessage(String.Format("{0} {1}", current_work, output));
                        }
                    }

                    // Note: The child process waits until everything is read from the standard output -> A Deadlock could arise here
                    using (var reader = new StringReader(process.StandardOutput.ReadToEnd()))
                    {
                        string output;

                        while ((output = reader.ReadLine()) != null)
                        {
                            nd.writeLogMessage(MessageLevel.Debug, output);

                            if (String.IsNullOrEmpty(output) == false)
                            {
                                nd.logMessage(output, false);
                            }
                        }
                    }

                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                    throw;
                }


                if (process.ExitCode != 0)
                {
                    throw new MagellanProcessingException(
                        String.Format("The exit code of {0} was {1}. (The expected exit code is 0)",
                                      Path.GetFileName(process.StartInfo.FileName),
                                      process.ExitCode));
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                throw;
            }
            catch (Exception ex)
            {
                nd.errorLog(ex, "The following exception was raised during the execution of \"{0}\":", exec_path);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    nd.warnLog("The process [{0}] hasn't finished correctly -> force to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }

            nd.logMessage(String.Format("{0} tool processing took {1}.", exec_path, StringHelper.GetDisplayString(timer.Elapsed)));
        }

        /// <summary>
        /// Return a modified sequence string (OpenMS format) from unmodified sequence + Thermo-formatted modification string
        /// </summary>
        public static string ModSequence(string seq, string mods_str)
        {
            if (mods_str == "") return seq;

            string[] mods = Regex.Split(mods_str, "; ");
            var actual_mods = new List<string>();

            // WORKAROUND: PD uses "modifications" like "X1(L)" to indicate that AA X at pos. 1 is actually a leucine
            // Substitute these before doing anything else, since "X" AAs might also be actually modified in addition
            // to the "modifications" indicating the actual AA.
            var tmp_seq = new StringBuilder(seq);
            string aa_letters = "ARNDCEQGHILKMFPSTWYV";
            foreach (string m in mods)
            {
                bool actual_mod = true;
                // m is something like "M11(Oxidation)" or "N-Term(Carbamyl)" or "X8(L)"
                string[] parts = m.Split('(');

                if (!aa_letters.Contains(parts[0].Substring(0, 1)))
                {
                    // modified AA character is not an actual AA (probably B, J, X, or Z)
                    // ==> now, also check if "modification" consists of just 1 letter representing an AA
                    if (parts[1].Length == 2 && aa_letters.Contains(parts[1][0]))
                    {
                        // substitute
                        Int32 aa_pos = Convert.ToInt32(parts[0].Substring(1));
                        tmp_seq[aa_pos - 1] = parts[1][0];
                        // discard this "modification"
                        actual_mod = false;
                    }
                }
                if (actual_mod)
                {
                    actual_mods.Add(m);
                }
            }
            var unmodified_seq = tmp_seq.ToString();

            var result = "";
            var n_term_mod = "";
            var c_term_mod = "";
            Int32 last_pos = 0;

            // assumption: modifications are in ascending order of AA position
            foreach (string mm in actual_mods)
            {
                // remove (Prot) if present (e.g. "N-Term(Prot)(Acetyl)")
                var m = mm.Replace("(Prot)", "");

                // have something like "M11(Oxidation)" or "N-Term(Carbamyl)"
                string[] parts = m.Split('(');

                // N-term
                if (parts[0].Length >= 6 && parts[0].Substring(0, 6) == "N-Term")
                {
                    n_term_mod = "(" + parts[1];
                    continue;
                }
                // C-term
                if (parts[0].Length >= 6 && parts[0].Substring(0, 6) == "C-Term")
                {
                    c_term_mod = "(" + parts[1];
                    continue;
                }
                // Residue
                Int32 aa_pos = Convert.ToInt32(parts[0].Substring(1));
                Int32 substr_len = aa_pos - last_pos;
                string mod_str = "(" + parts[1];
                string next_chunk = unmodified_seq.Substring(last_pos, substr_len);
                result += next_chunk + mod_str;
                last_pos = aa_pos;
            }
            result = n_term_mod + result + unmodified_seq.Substring(last_pos) + c_term_mod;

            return result;
        }

        /// <summary>
        /// Remove obsolete CV terms from a set of mzML files that would otherwise lead to delay and millions of warning messages on loading in OpenMS.
        /// </summary>
        public static void FixCVTerms(List<string> mzml_files, NodeDelegates nd)
        {
            // remove in particular one obsolete CV term (MS:1000498)
            // which otherwise leads to huge delay and millions of warning
            // messages when loading the file into OpenMS
            foreach (var f in mzml_files)
            {
                // move to temporary file
                var tmp_f = f.Replace(".mzML", "_tmp.mzML");
                try
                {
                    File.Move(f, tmp_f);
                }
                catch (Exception)
                {
                    nd.errorLogMessage(string.Format("Could not move file {0} to {1}", f, tmp_f));
                }

                // open temporary file, remove obsolete CV terms, store with original filename
                XDocument doc = XDocument.Load(tmp_f);
                var q = from node in doc.Descendants("{http://psi.hupo.org/ms/mzml}cvParam")
                        let acc = node.Attribute("accession")
                        where acc != null && acc.Value == "MS:1000498"
                        select node;
                q.ToList().ForEach(x => x.Remove());
                try
                {
                    doc.Save(f);
                }
                catch (Exception)
                {
                    nd.errorLogMessage(string.Format("Could not save file {0}", f));
                }

                // remove temporary file
                try
                {
                    File.Delete(tmp_f);
                }
                catch (Exception)
                {
                    nd.errorLogMessage(string.Format("Could not delete file {0}", tmp_f));
                }
            }
        }
    }
}
