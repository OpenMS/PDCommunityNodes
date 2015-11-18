using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Xml;
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
    public delegate void NodeLoggerWarningDelegate(string s, params object[] args);
    public delegate Exception NodeLoggerErrorDelegate(Exception e, string s, params object[] args);
    public delegate void SendAndLogTemporaryMessageDelegate(string s, bool writeToLog = true);
    public delegate void SendAndLogMessageDelegate(string s, bool writeToLog = true);
    public delegate void WriteLogMessageDelegate(MessageLevel ml, string s);

    public class OpenMSCommons
    {
        public static void CreateDefaultINI(string exec_path, string ini_path, string scratch_dir, NodeLoggerErrorDelegate errorLog, NodeLoggerWarningDelegate warnLog)
        {
            var timer = Stopwatch.StartNew();

            var process = new Process
            {
                StartInfo =
                {
                    FileName = exec_path,
                    WorkingDirectory = scratch_dir,
                    Arguments = " -write_ini " + String.Format("\"{0}\"", ini_path),
                    UseShellExecute = false,
                    RedirectStandardOutput = false,
                    CreateNoWindow = false
                }
            };

            try
            {
                process.Start();

                try
                {
                    //process.PriorityClass = ProcessPriorityClass.BelowNormal;
                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    errorLog(ex, "The following exception is raised during the execution of \"{0}\":", exec_path);
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
                errorLog(ex, "The following exception is raised during the execution of \"{0}\":", exec_path);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    warnLog("The process [{0}] isn't finished correctly -> force the process to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }
        }

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

        //Write ITEMLISTs, used for input or output file lists
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

        //Write mz and rt parameters. different function than WriteParamsToINI due to specific structure in considered tools
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
                    item.Attributes["value"].Value = (rt_threshold.Value * 60).ToString(); //need to convert minute(CD) to seconds(OpenMS)!
                }
            }
            doc.Save(ini_path);
        }

        //execute specific OpenMS Tool (exec_path) with specified Ini (param_path)        
        public static void RunTOPPTool(string exec_path,
                                       string param_path,
                                       string scratch_dir,
                                       SendAndLogMessageDelegate logMessage,
                                       SendAndLogTemporaryMessageDelegate logTmpMessage,
                                       WriteLogMessageDelegate writeLogMessage,
                                       NodeLoggerWarningDelegate nodeLogWarning,
                                       NodeLoggerErrorDelegate nodeLogError)
        {
            var timer = Stopwatch.StartNew();

            var process = new Process
            {
                StartInfo =
                {
                    FileName = exec_path,
                    WorkingDirectory = scratch_dir,
                    Arguments = " -ini " + String.Format("\"{0}\"", param_path),
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = false,
                    //WindowStyle = ProcessWindowStyle.Hidden
                }
            };

            logTmpMessage(String.Format("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                                        process.StartInfo.FileName,
                                        process.StartInfo.WorkingDirectory,
                                        process.StartInfo.Arguments));

            writeLogMessage(MessageLevel.Debug,
                            String.Format("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                                          process.StartInfo.FileName,
                                          process.StartInfo.WorkingDirectory,
                                          process.StartInfo.Arguments));

            try
            {
                process.Start();

                try
                {
                    //process.PriorityClass = ProcessPriorityClass.BelowNormal;

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
                        writeLogMessage(MessageLevel.Debug, output);

                        // Parse the output and report progress using the method SendAndLogTemporaryMessage
                        if (output.Contains(@"Progress of 'loading mzML file':"))
                        {
                            current_work = "Progress of 'loading mzML file':";
                        }
                        else if (output.Contains("Progress of 'loading chromatograms':"))
                        {
                            current_work = "Progress of 'loading chromatograms':";
                        }
                        else if (output.Contains("Progress of 'mass trace detection':"))
                        {
                            current_work = "Progress of 'mass trace detection':";
                        }
                        else if (output.Contains("Progress of 'elution peak detection':"))
                        {
                            current_work = "Progress of 'elution peak detection':";
                        }
                        else if (output.Contains("Progress of 'assembling mass traces to features':"))
                        {
                            current_work = "Progress of 'assembling mass traces to features':";
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
                            logTmpMessage(String.Format("{0} {1}", current_work, output));
                        }
                    }

                    // Note: The child process waits until everything is read from the standard output -> A Deadlock could arise here
                    using (var reader = new StringReader(process.StandardOutput.ReadToEnd()))
                    {
                        string output;

                        while ((output = reader.ReadLine()) != null)
                        {
                            writeLogMessage(MessageLevel.Debug, output);

                            if (String.IsNullOrEmpty(output) == false)
                            {
                                logMessage(output, false);
                            }
                        }
                    }

                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    nodeLogError(ex, "The following exception is raised during the execution of \"{0}\":", exec_path);
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
                nodeLogError(ex, "The following exception is raised during the execution of \"{0}\":", exec_path);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    nodeLogWarning("The process [{0}] isn't finished correctly -> force the process to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }

            logMessage(String.Format("{0} tool processing took {1}.", exec_path, StringHelper.GetDisplayString(timer.Elapsed)));
        }

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
                // have something like "M11(Oxidation) or N-Term(Carbamyl) or X8(L)"
                string[] parts = m.Split('(');

                if (!aa_letters.Contains(parts[0].Substring(0,1)))
                {
                    // modified AA character is not an actual AA (probably B, J, X, or Z)
                    // ==> now, also check if "modification" consists of just 1 letter representing an AA
                    if (parts[1].Length == 2 && aa_letters.Contains(parts[1][0]))
                    {
                        // substitute
                        Int32 aa_pos = Convert.ToInt32(parts[0].Substring(1));
                        tmp_seq[aa_pos - 1] = parts[1][0]; //TODO bounds check
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
            //assumption: modifications are in ascending order of AA position
            foreach (string m in actual_mods)
            {
                // have something like "M11(Oxidation) or N-Term(Carbamyl)"
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
    }
}
